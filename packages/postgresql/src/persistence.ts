import pg, { type Pool, type PoolClient } from 'pg';
import {
  MissingResourcesError,
  SessionCommitConflictError,
  silentLogger,
  type Logger,
  type OutboxRecord,
  type PersistenceProvider,
} from '@zusammen/core';
import { fromRow, toOperationsJson, type OutboxRow } from './rows.js';

export const DEFAULT_OUTBOX_TABLE = 'zusammen_outbox';
export const DEFAULT_SCHEMA = 'public';
export const DEFAULT_RETENTION_MS = 7 * 24 * 60 * 60 * 1_000;
export const DEFAULT_CLEANUP_INTERVAL_MS = 60 * 60 * 1_000;

const UNIQUE_VIOLATION = '23505';
const SERIALIZATION_FAILURE = '40001';
const CLEANUP_BATCH_SIZE = 1_000;

export type IsolationLevel = 'read committed' | 'repeatable read' | 'serializable';

export interface PostgreSQLPersistenceOptions {
  /** The pool your application uses: business queries must run on the session's client to join its transaction. */
  pool: Pool;
  /** Must exist; defaults to {@link DEFAULT_SCHEMA}. */
  schemaName?: string | undefined;
  /** Defaults to {@link DEFAULT_OUTBOX_TABLE}. */
  tableName?: string | undefined;
  /** How long dispatched records and tombstones are kept. Defaults to 7 days. */
  retentionMs?: number | undefined;
  /** How often expired records are deleted, from `connect()` to `disconnect()`; 0 disables it. Defaults to 1 hour. */
  cleanupIntervalMs?: number | undefined;
  /** Defaults to the server's default (usually read committed). */
  isolationLevel?: IsolationLevel | undefined;
  /** Whether `disconnect()` ends the pool. Defaults to false: the pool is usually shared with the application. */
  closePoolOnDisconnect?: boolean | undefined;
  logger?: Logger | undefined;
}

/** PostgreSQL persistence. The transaction context is the `PoolClient` to run your own queries on. */
export class PostgreSQLPersistence implements PersistenceProvider<PoolClient> {
  readonly #pool: Pool;
  readonly #schemaName: string;
  readonly #tableName: string;
  readonly #table: string;
  readonly #indexName: string;
  readonly #retentionMs: number;
  readonly #cleanupIntervalMs: number;
  readonly #begin: string;
  readonly #closePoolOnDisconnect: boolean;
  readonly #logger: Logger;
  // Clients with an open transaction: commit and rollback release them, later calls are no-ops
  readonly #open = new WeakSet<PoolClient>();
  #cleanupTimer: NodeJS.Timeout | undefined;

  constructor(options: PostgreSQLPersistenceOptions) {
    this.#pool = options.pool;
    this.#schemaName = options.schemaName ?? DEFAULT_SCHEMA;
    this.#tableName = options.tableName ?? DEFAULT_OUTBOX_TABLE;
    this.#table = `${quote(this.#schemaName)}.${quote(this.#tableName)}`;
    this.#indexName = `${this.#tableName}_dispatched_at`;
    this.#retentionMs = options.retentionMs ?? DEFAULT_RETENTION_MS;
    this.#cleanupIntervalMs = options.cleanupIntervalMs ?? DEFAULT_CLEANUP_INTERVAL_MS;
    this.#begin = options.isolationLevel === undefined ? 'BEGIN' : `BEGIN ISOLATION LEVEL ${options.isolationLevel}`;
    this.#closePoolOnDisconnect = options.closePoolOnDisconnect ?? false;
    this.#logger = options.logger ?? silentLogger;
  }

  /** The schema-qualified, quoted outbox table name, e.g. for monitoring queries. */
  get table(): string {
    return this.#table;
  }

  async connect(): Promise<void> {
    await this.#pool.query('SELECT 1');
    if (this.#cleanupIntervalMs > 0 && this.#cleanupTimer === undefined) {
      this.#cleanupTimer = setInterval(() => {
        this.removeExpired().catch((error: unknown) => {
          this.#logger.warn('Removing expired outbox records failed', { table: this.#table, error });
        });
      }, this.#cleanupIntervalMs);
      this.#cleanupTimer.unref();
    }
  }

  async disconnect(): Promise<void> {
    clearInterval(this.#cleanupTimer);
    this.#cleanupTimer = undefined;
    if (this.#closePoolOnDisconnect) {
      await this.#pool.end();
    }
  }

  /** Creates the outbox table and its cleanup index; the schema must exist. */
  async createResources(): Promise<void> {
    const client = await this.#pool.connect();
    try {
      await client.query('BEGIN');
      // Concurrent CREATE ... IF NOT EXISTS can still fail on the catalog's unique indexes: serialize instances
      await client.query('SELECT pg_advisory_xact_lock(hashtext($1))', [this.#table]);
      await client.query(
        `CREATE TABLE IF NOT EXISTS ${this.#table} (
          id text PRIMARY KEY,
          dispatched boolean NOT NULL DEFAULT false,
          dispatched_at timestamptz,
          transport_operations jsonb NOT NULL
        )`,
      );
      await client.query(
        `CREATE INDEX IF NOT EXISTS ${quote(this.#indexName)} ON ${this.#table} (dispatched_at) WHERE dispatched`,
      );
      await client.query('COMMIT');
    } catch (error) {
      await client.query('ROLLBACK').catch(() => undefined);
      throw error;
    } finally {
      client.release();
    }
  }

  /** The outbox table must exist; a missing cleanup index only makes cleanup slower, so it's logged. */
  async verifyResources(): Promise<void> {
    const { rows } = await this.#pool.query<{ table: string | null; index: string | null }>(
      'SELECT to_regclass($1)::text AS table, to_regclass($2)::text AS index',
      [this.#table, `${quote(this.#schemaName)}.${quote(this.#indexName)}`],
    );
    if (rows[0]?.table === null) {
      throw new MissingResourcesError([`PostgreSQL table '${this.#schemaName}.${this.#tableName}'`]);
    }
    if (rows[0]?.index === null) {
      this.#logger.warn('The outbox table has no index on dispatched_at; cleanup scans the whole table', {
        table: this.#table,
      });
    }
  }

  async begin(): Promise<PoolClient> {
    const client = await this.#pool.connect();
    try {
      await client.query(this.#begin);
    } catch (error) {
      client.release(true);
      throw error;
    }
    this.#open.add(client);
    return client;
  }

  async storeOutbox(record: OutboxRecord, client: PoolClient): Promise<void> {
    try {
      await client.query(
        `INSERT INTO ${this.#table} (id, dispatched, dispatched_at, transport_operations) VALUES ($1, $2, $3, $4::jsonb)`,
        [record.id, record.dispatched, record.dispatchedAt ?? null, toOperationsJson(record.transportOperations)],
      );
    } catch (error) {
      // A tombstone committed first; under serializable isolation it can surface as a serialization failure
      const code = sqlState(error);
      if (code === UNIQUE_VIOLATION || code === SERIALIZATION_FAILURE) {
        throw new SessionCommitConflictError(record.id, { cause: error });
      }
      throw error;
    }
  }

  async commit(client: PoolClient): Promise<void> {
    if (!this.#open.delete(client)) {
      throw new Error('The transaction was already committed or rolled back');
    }
    const result = await end(client, 'COMMIT');
    // PostgreSQL answers COMMIT of a transaction aborted by an earlier error with a silent rollback
    if (result.command === 'ROLLBACK') {
      throw new Error('The transaction was aborted by an earlier error and has been rolled back');
    }
  }

  async rollback(client: PoolClient): Promise<void> {
    if (!this.#open.delete(client)) {
      return;
    }
    await end(client, 'ROLLBACK');
  }

  async get(sessionId: string): Promise<OutboxRecord | null> {
    const { rows } = await this.#pool.query<OutboxRow>(
      `SELECT id, dispatched, dispatched_at, transport_operations FROM ${this.#table} WHERE id = $1`,
      [sessionId],
    );
    return rows[0] === undefined ? null : fromRow(rows[0]);
  }

  async markDispatched(sessionId: string): Promise<void> {
    await this.#pool.query(
      `UPDATE ${this.#table} SET dispatched = true, dispatched_at = $2 WHERE id = $1 AND NOT dispatched`,
      [sessionId, new Date()],
    );
  }

  async storeTombstone(sessionId: string): Promise<'stored' | 'exists'> {
    // Waits for an in-flight transaction that inserted the same ID: exists if it commits, stored if it rolls back
    const result = await this.#pool.query(
      `INSERT INTO ${this.#table} (id, dispatched, dispatched_at, transport_operations)
       VALUES ($1, true, $2, '[]'::jsonb) ON CONFLICT (id) DO NOTHING`,
      [sessionId, new Date()],
    );
    return result.rowCount === 1 ? 'stored' : 'exists';
  }

  /**
   * Deletes dispatched records and tombstones older than the retention, in batches; returns how many. Runs every
   * `cleanupIntervalMs` while connected; call it from a scheduled job instead when that's disabled.
   */
  async removeExpired(): Promise<number> {
    const cutoff = new Date(Date.now() - this.#retentionMs);
    let removed = 0;
    for (;;) {
      const result = await this.#pool.query(
        `DELETE FROM ${this.#table} WHERE id IN (
           SELECT id FROM ${this.#table} WHERE dispatched AND dispatched_at < $1 LIMIT $2 FOR UPDATE SKIP LOCKED
         )`,
        [cutoff, CLEANUP_BATCH_SIZE],
      );
      removed += result.rowCount ?? 0;
      if ((result.rowCount ?? 0) < CLEANUP_BATCH_SIZE) {
        return removed;
      }
    }
  }
}

// Releases the client to the pool, or discards it if the connection may be broken
async function end(client: PoolClient, statement: 'COMMIT' | 'ROLLBACK'): Promise<pg.QueryResult> {
  try {
    const result = await client.query(statement);
    client.release();
    return result;
  } catch (error) {
    client.release(!(error instanceof pg.DatabaseError));
    throw error;
  }
}

function quote(identifier: string): string {
  return `"${identifier.replaceAll('"', '""')}"`;
}

/** The SQLSTATE of a server error, undefined for anything else (e.g. a broken connection). */
function sqlState(error: unknown): string | undefined {
  return error instanceof pg.DatabaseError ? error.code : undefined;
}
