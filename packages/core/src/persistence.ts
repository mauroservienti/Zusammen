import type { OutboxRecord } from './outbox.js';

/**
 * Stores outbox records and owns the business transaction. `TContext` is what users receive as
 * `session.transactionContext` to enlist their own operations (e.g. a MongoDB `ClientSession`).
 */
export interface PersistenceProvider<TContext> {
  connect(): Promise<void>;
  disconnect(): Promise<void>;
  /** Creates the resources the provider needs (e.g. collections, indexes); idempotent. Requires `connect()`. */
  createResources?(): Promise<void>;
  /** Throws {@link MissingResourcesError} if resources are missing. Requires `connect()`. */
  verifyResources?(): Promise<void>;

  begin(): Promise<TContext>;
  /** Inserts the record within the transaction. Fails with {@link SessionCommitConflictError} if the ID exists (tombstone). */
  storeOutbox(record: OutboxRecord, context: TContext): Promise<void>;
  /** Fails with {@link SessionCommitConflictError} if the outbox insert conflicts at commit time. */
  commit(context: TContext): Promise<void>;
  rollback(context: TContext): Promise<void>;

  /** Must see every committed record (no stale reads), otherwise a committed session can be mistaken for a missing one. */
  get(sessionId: string): Promise<OutboxRecord | null>;
  markDispatched(sessionId: string): Promise<void>;
  /** Inserts an already-dispatched, empty record so a late commit of the same session fails. */
  storeTombstone(sessionId: string): Promise<'stored' | 'exists'>;
}
