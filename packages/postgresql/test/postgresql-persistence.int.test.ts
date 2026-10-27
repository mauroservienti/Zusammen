import { PostgreSqlContainer, type StartedPostgreSqlContainer } from '@testcontainers/postgresql';
import pg, { type PoolClient } from 'pg';
import { afterAll, beforeAll, describe, expect, test } from 'vitest';
import { PostgreSQLPersistence, type OutboxRow, type PostgreSQLPersistenceOptions } from '@zusammen/postgresql';
import { persistenceProviderContract, sampleRecord } from '@zusammen/testing';

let container: StartedPostgreSqlContainer;
let pool: pg.Pool;
let schema = 0;

beforeAll(async () => {
  container = await new PostgreSqlContainer('postgres:18').start();
  pool = new pg.Pool({ connectionString: container.getConnectionUri() });
}, 120_000);

afterAll(async () => {
  await pool.end();
  await container.stop();
});

async function createSchema() {
  const schemaName = `zusammen_${String(++schema)}`;
  await pool.query(`CREATE SCHEMA ${schemaName}`);
  return schemaName;
}

/** A provider on a fresh schema, so tests don't see each other's data. */
async function createPersistence(options: Partial<PostgreSQLPersistenceOptions> = {}) {
  const schemaName = await createSchema();
  const persistence = new PostgreSQLPersistence({ pool, schemaName, cleanupIntervalMs: 0, ...options });
  await persistence.connect();
  await persistence.createResources();
  return { persistence, schemaName };
}

persistenceProviderContract<PoolClient>('postgresql', {
  setup: async () => {
    const { persistence, schemaName } = await createPersistence();
    await pool.query(`CREATE TABLE ${schemaName}.business (key text PRIMARY KEY, value text NOT NULL)`);
    return {
      persistence,
      write: async (client, key, value) => {
        await client.query(`INSERT INTO ${schemaName}.business (key, value) VALUES ($1, $2)`, [key, value]);
      },
      read: async (key) => {
        const { rows } = await pool.query<{ value: string }>(
          `SELECT value FROM ${schemaName}.business WHERE key = $1`,
          [key],
        );
        return rows[0]?.value;
      },
    };
  },
});

describe('postgresql persistence', () => {
  test('creates nothing by default; verification reports the missing table', async () => {
    const schemaName = await createSchema();
    const persistence = new PostgreSQLPersistence({ pool, schemaName, cleanupIntervalMs: 0 });
    await persistence.connect();

    await expect(persistence.verifyResources()).rejects.toMatchObject({
      name: 'MissingResourcesError',
      resources: [`PostgreSQL table '${schemaName}.zusammen_outbox'`],
    });
    const { rows } = await pool.query('SELECT 1 FROM pg_tables WHERE schemaname = $1', [schemaName]);
    expect(rows).toEqual([]);
  });

  test('verification passes once resources exist; creating them twice, even concurrently, is fine', async () => {
    const schemaName = await createSchema();
    const instances = [1, 2, 3].map(() => new PostgreSQLPersistence({ pool, schemaName, cleanupIntervalMs: 0 }));
    await Promise.all(instances.map((instance) => instance.createResources()));
    await instances[0]?.createResources();

    await expect(instances[0]?.verifyResources()).resolves.toBeUndefined();
  });

  test('a missing cleanup index is only a warning', async () => {
    const { schemaName } = await createPersistence();
    await pool.query(`DROP INDEX ${schemaName}.zusammen_outbox_dispatched_at`);
    const warnings: string[] = [];
    const verifying = new PostgreSQLPersistence({
      pool,
      schemaName,
      cleanupIntervalMs: 0,
      logger: { debug() {}, info() {}, error() {}, warn: (message) => warnings.push(message) },
    });

    await expect(verifying.verifyResources()).resolves.toBeUndefined();
    expect(warnings).toEqual([expect.stringContaining('no index on dispatched_at')]);
  });

  test('supports table names that need quoting', async () => {
    const { persistence } = await createPersistence({ tableName: 'Outbox "quoted"' });
    const record = sampleRecord();
    const client = await persistence.begin();
    await persistence.storeOutbox(record, client);
    await persistence.commit(client);

    expect(await persistence.get(record.id)).toEqual(record);
    await expect(persistence.verifyResources()).resolves.toBeUndefined();
  });

  test('stores transport operations as JSON with base64 bodies', async () => {
    const { persistence } = await createPersistence();
    const record = sampleRecord();
    const client = await persistence.begin();
    await persistence.storeOutbox(record, client);
    await persistence.commit(client);

    const { rows } = await pool.query<OutboxRow>(`SELECT * FROM ${persistence.table} WHERE id = $1`, [record.id]);
    expect(rows[0]).toMatchObject({ dispatched: false, dispatched_at: null });
    expect(rows[0]?.transport_operations[0]).toMatchObject({
      headers: { 'with.dots': 'and $dollars' },
      body: Buffer.from([0, 1, 2, 250, 255]).toString('base64'),
    });
  });

  test('a tombstone waits for an in-flight transaction that already inserted the record', async () => {
    const { persistence } = await createPersistence();
    const record = sampleRecord();
    const client = await persistence.begin();
    await persistence.storeOutbox(record, client);

    let settled = false;
    const tombstone = persistence.storeTombstone(record.id).finally(() => {
      settled = true;
    });
    await new Promise((resolve) => setTimeout(resolve, 300));
    expect(settled).toBe(false);

    await persistence.commit(client);
    expect(await tombstone).toBe('exists');
    expect(await persistence.get(record.id)).toEqual(record);
  });

  test('a tombstone waiting on a transaction that rolls back is stored', async () => {
    const { persistence } = await createPersistence();
    const record = sampleRecord();
    const client = await persistence.begin();
    await persistence.storeOutbox(record, client);

    const tombstone = persistence.storeTombstone(record.id);
    await new Promise((resolve) => setTimeout(resolve, 300));
    await persistence.rollback(client);

    expect(await tombstone).toBe('stored');
  });

  test('committing a transaction aborted by an earlier error fails instead of silently rolling back', async () => {
    const { persistence } = await createPersistence();
    const client = await persistence.begin();
    await expect(client.query('SELECT * FROM missing_table')).rejects.toThrow();

    await expect(persistence.commit(client)).rejects.toThrow('aborted by an earlier error');
    await expect(persistence.rollback(client)).resolves.toBeUndefined();
  });

  test('releases clients to the pool on commit and rollback', async () => {
    const { persistence } = await createPersistence();
    const before = pool.totalCount;
    for (let i = 0; i < 20; i++) {
      const client = await persistence.begin();
      await (i % 2 === 0 ? persistence.commit(client) : persistence.rollback(client));
    }

    expect(pool.totalCount).toBeLessThanOrEqual(Math.max(before, 1) + 1);
    expect(pool.waitingCount).toBe(0);
  });

  test('applies the configured isolation level', async () => {
    const { persistence } = await createPersistence({ isolationLevel: 'serializable' });
    const client = await persistence.begin();
    const { rows } = await client.query<{ transaction_isolation: string }>('SHOW transaction_isolation');
    await persistence.rollback(client);

    expect(rows[0]?.transaction_isolation).toBe('serializable');
  });

  test('a session started before the tombstone cannot commit under serializable isolation either', async () => {
    const { persistence } = await createPersistence({ isolationLevel: 'serializable' });
    const record = sampleRecord();
    const client = await persistence.begin();
    await client.query(`SELECT count(*) FROM ${persistence.table}`); // takes the snapshot
    await persistence.storeTombstone(record.id);

    await expect(persistence.storeOutbox(record, client)).rejects.toMatchObject({
      name: 'SessionCommitConflictError',
    });
    await persistence.rollback(client);
  });

  test('removeExpired deletes dispatched records and tombstones past the retention, keeping the rest', async () => {
    const { persistence } = await createPersistence({ retentionMs: 60_000 });
    const pending = sampleRecord();
    const recent = sampleRecord();
    const expired = sampleRecord();
    for (const record of [pending, recent, expired]) {
      const client = await persistence.begin();
      await persistence.storeOutbox(record, client);
      await persistence.commit(client);
    }
    await persistence.markDispatched(recent.id);
    await persistence.markDispatched(expired.id);
    await persistence.storeTombstone('expired-tombstone');
    await pool.query(
      `UPDATE ${persistence.table} SET dispatched_at = now() - interval '2 minutes' WHERE id = ANY($1)`,
      [[expired.id, 'expired-tombstone']],
    );
    // Not dispatched: kept however old
    await pool.query(`UPDATE ${persistence.table} SET dispatched_at = now() - interval '1 day' WHERE id = $1`, [
      pending.id,
    ]);

    expect(await persistence.removeExpired()).toBe(2);
    expect(await persistence.get(expired.id)).toBeNull();
    expect(await persistence.get('expired-tombstone')).toBeNull();
    expect(await persistence.get(recent.id)).not.toBeNull();
    expect(await persistence.get(pending.id)).not.toBeNull();
  });

  test('removes expired records periodically while connected', async () => {
    const { persistence } = await createPersistence({ retentionMs: 1, cleanupIntervalMs: 100 });
    await persistence.storeTombstone('old-tombstone');

    await expect.poll(() => persistence.get('old-tombstone'), { timeout: 5_000 }).toBeNull();
    await persistence.disconnect();
  });

  test('does not end a shared pool on disconnect', async () => {
    const { persistence } = await createPersistence();
    await persistence.disconnect();

    await expect(pool.query('SELECT 1')).resolves.toBeDefined();
  });
});
