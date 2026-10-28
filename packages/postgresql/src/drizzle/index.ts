import { sql, type SQL, type SQLChunk } from 'drizzle-orm';
import type { ApplicationTransactions } from '../persistence.js';

/** A Drizzle PostgreSQL database, with any driver (`node-postgres`, `postgres-js`, Neon, ...). */
export interface DrizzleDatabase<TTransaction> {
  transaction<T>(transaction: (tx: TTransaction) => Promise<T>): Promise<T>;
}

/** The transaction object Drizzle's `db.transaction(fn)` passes to `fn`. */
export type DrizzleTransaction<TDatabase> =
  TDatabase extends DrizzleDatabase<infer TTransaction> ? TTransaction : never;

interface DrizzleExecutor {
  execute(query: SQL): PromiseLike<unknown>;
}

/**
 * Runs the outbox insert in Drizzle transactions, for `PostgreSQLOrmPersistence`. `db` only gives the transaction
 * type: `new PostgreSQLOrmPersistence({ pool, transactions: drizzleTransactions(db) })`.
 */
export function drizzleTransactions<TTransaction extends DrizzleExecutor>(
  db: DrizzleDatabase<TTransaction>,
): ApplicationTransactions<TTransaction>;
export function drizzleTransactions(): ApplicationTransactions<DrizzleExecutor> {
  return {
    execute: async (tx, statement, parameters) => tx.execute(toSql(statement, parameters)),
  };
}

// "... VALUES ($1, $2)" with parameters → Drizzle SQL with the same parameters bound
function toSql(statement: string, parameters: unknown[]): SQL {
  const chunks: SQLChunk[] = statement
    .split(/\$(\d+)/)
    .map((part, index) => (index % 2 === 0 ? sql.raw(part) : sql`${parameters[Number(part) - 1]}`));
  return sql.join(chunks);
}
