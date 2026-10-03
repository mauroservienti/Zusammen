import {
  MongoServerError,
  ReadConcern,
  ReadPreference,
  WriteConcern,
  type ClientSession,
  type Collection,
  type MongoClient,
  type TransactionOptions,
} from 'mongodb';
import {
  MissingResourcesError,
  SessionCommitConflictError,
  silentLogger,
  type Logger,
  type OutboxRecord,
  type PersistenceProvider,
} from '@zusammen/core';
import { fromDocument, toDocument, type OutboxDocument } from './documents.js';

export const DEFAULT_OUTBOX_COLLECTION = 'zusammen_outbox';
export const DEFAULT_RETENTION_MS = 7 * 24 * 60 * 60 * 1_000;
const TTL_INDEX_NAME = 'zusammen_dispatched_ttl';

const DUPLICATE_KEY = 11000;
const WRITE_CONFLICT = 112;
const INDEX_OPTIONS_CONFLICT = 85;
const NAMESPACE_EXISTS = 48;
const MAX_COMMIT_ATTEMPTS = 3;

export interface MongoDBPersistenceOptions {
  /** The client your application uses: business operations must use sessions from the same client. */
  client: MongoClient;
  /** Defaults to the database in the connection string. */
  databaseName?: string | undefined;
  /** Defaults to {@link DEFAULT_OUTBOX_COLLECTION}. */
  collectionName?: string | undefined;
  /** How long dispatched records and tombstones are kept. Defaults to 7 days. */
  retentionMs?: number | undefined;
  /** Defaults to snapshot reads, majority writes, primary reads. */
  transactionOptions?: TransactionOptions | undefined;
  /** Whether `disconnect()` closes the client. Defaults to false: the client is usually shared with the application. */
  closeClientOnDisconnect?: boolean | undefined;
  logger?: Logger | undefined;
}

/** MongoDB persistence. The transaction context is the `ClientSession` to pass to your own operations. */
export class MongoDBPersistence implements PersistenceProvider<ClientSession> {
  readonly #client: MongoClient;
  readonly #collection: Collection<OutboxDocument>;
  readonly #retentionMs: number;
  readonly #transactionOptions: TransactionOptions;
  readonly #closeClientOnDisconnect: boolean;
  readonly #logger: Logger;

  constructor(options: MongoDBPersistenceOptions) {
    this.#client = options.client;
    // Control path reads and writes must see every committed transaction: primary, majority
    this.#collection = options.client
      .db(options.databaseName)
      .collection<OutboxDocument>(options.collectionName ?? DEFAULT_OUTBOX_COLLECTION, {
        readPreference: ReadPreference.primary,
        readConcern: new ReadConcern('majority'),
        writeConcern: new WriteConcern('majority'),
      });
    this.#retentionMs = options.retentionMs ?? DEFAULT_RETENTION_MS;
    this.#transactionOptions = options.transactionOptions ?? {
      readConcern: new ReadConcern('snapshot'),
      writeConcern: new WriteConcern('majority'),
      readPreference: ReadPreference.primary,
    };
    this.#closeClientOnDisconnect = options.closeClientOnDisconnect ?? false;
    this.#logger = options.logger ?? silentLogger;
  }

  /** The outbox collection, e.g. for monitoring. */
  get collection(): Collection<OutboxDocument> {
    return this.#collection;
  }

  async connect(): Promise<void> {
    await this.#client.connect();
  }

  /** Creates the outbox collection and its TTL index; updates the TTL if the retention changed. */
  async createResources(): Promise<void> {
    try {
      await this.#collection.db.createCollection(this.#collection.collectionName);
    } catch (error) {
      if (!(error instanceof MongoServerError && error.code === NAMESPACE_EXISTS)) throw error;
    }
    await this.#ensureTtlIndex();
  }

  /** The outbox collection must exist; a missing TTL index only means records are never cleaned up, so it's logged. */
  async verifyResources(): Promise<void> {
    const name = this.#collection.collectionName;
    const exists = await this.#collection.db.listCollections({ name }, { nameOnly: true }).hasNext();
    if (!exists) {
      throw new MissingResourcesError([`MongoDB collection '${this.#collection.dbName}.${name}'`]);
    }
    const indexes = await this.#collection.indexes();
    if (!indexes.some((index) => index.expireAfterSeconds !== undefined && index.key.DispatchedAt === 1)) {
      this.#logger.warn(
        'The outbox collection has no TTL index on DispatchedAt; dispatched records are never removed',
        {
          collection: `${this.#collection.dbName}.${name}`,
        },
      );
    }
  }

  async disconnect(): Promise<void> {
    if (this.#closeClientOnDisconnect) {
      await this.#client.close();
    }
  }

  begin(): Promise<ClientSession> {
    const session = this.#client.startSession();
    session.startTransaction(this.#transactionOptions);
    return Promise.resolve(session);
  }

  async storeOutbox(record: OutboxRecord, session: ClientSession): Promise<void> {
    outboxIds.set(session, record.id);
    try {
      await this.#collection.insertOne(toDocument(record), { session });
    } catch (error) {
      // Within a transaction, a concurrent tombstone shows up as a duplicate key or a write conflict on this insert
      if (error instanceof MongoServerError && (error.code === DUPLICATE_KEY || error.code === WRITE_CONFLICT)) {
        throw new SessionCommitConflictError(record.id, { cause: error });
      }
      throw error;
    }
  }

  async commit(session: ClientSession): Promise<void> {
    try {
      for (let attempt = 1; ; attempt++) {
        try {
          await session.commitTransaction();
          return;
        } catch (error) {
          // Safe to retry: commitTransaction is idempotent for this outcome
          const unknownResult =
            error instanceof MongoServerError && error.hasErrorLabel('UnknownTransactionCommitResult');
          if (!unknownResult || attempt >= MAX_COMMIT_ATTEMPTS) {
            throw error;
          }
        }
      }
    } catch (error) {
      const outboxId = outboxIds.get(session);
      if (outboxId !== undefined && error instanceof MongoServerError && error.code === DUPLICATE_KEY) {
        throw new SessionCommitConflictError(outboxId, { cause: error });
      }
      throw error;
    } finally {
      await session.endSession();
    }
  }

  async rollback(session: ClientSession): Promise<void> {
    if (session.hasEnded) {
      return;
    }
    try {
      if (session.inTransaction()) {
        await session.abortTransaction();
      }
    } finally {
      await session.endSession();
    }
  }

  async get(sessionId: string): Promise<OutboxRecord | null> {
    const document = await this.#collection.findOne({ _id: sessionId });
    return document === null ? null : fromDocument(document);
  }

  async markDispatched(sessionId: string): Promise<void> {
    await this.#collection.updateOne(
      { _id: sessionId, Dispatched: false },
      { $set: { Dispatched: true, DispatchedAt: new Date() } },
    );
  }

  async storeTombstone(sessionId: string): Promise<'stored' | 'exists'> {
    try {
      await this.#collection.insertOne({
        _id: sessionId,
        Dispatched: true,
        DispatchedAt: new Date(),
        TransportOperations: [],
      });
      return 'stored';
    } catch (error) {
      if (error instanceof MongoServerError && error.code === DUPLICATE_KEY) {
        return 'exists';
      }
      throw error;
    }
  }

  async #ensureTtlIndex(): Promise<void> {
    const expireAfterSeconds = Math.ceil(this.#retentionMs / 1_000);
    try {
      await this.#collection.createIndex({ DispatchedAt: 1 }, { name: TTL_INDEX_NAME, expireAfterSeconds });
    } catch (error) {
      if (!(error instanceof MongoServerError && error.code === INDEX_OPTIONS_CONFLICT)) {
        throw error;
      }
      // Retention changed since the index was created
      await this.#collection.db.command({
        collMod: this.#collection.collectionName,
        index: { name: TTL_INDEX_NAME, expireAfterSeconds },
      });
    }
  }
}

// The outbox record ID stored by each session, to report commit conflicts
const outboxIds = new WeakMap<ClientSession, string>();
