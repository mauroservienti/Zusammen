import { CompiledQuery, type Kysely, type Transaction } from 'kysely';
import type { ApplicationTransactions } from '../persistence.js';

/**
 * Runs the outbox insert in Kysely transactions (`db.transaction().execute(fn)`), for `PostgreSQLOrmPersistence`.
 * `db` only gives the database type: `new PostgreSQLOrmPersistence({ pool, transactions: kyselyTransactions(db) })`.
 */
export function kyselyTransactions<TDatabase>(db: Kysely<TDatabase>): ApplicationTransactions<Transaction<TDatabase>>;
export function kyselyTransactions(): ApplicationTransactions<Transaction<unknown>> {
  // The PostgreSQL dialect takes `$1`-style parameters, and Kysely rethrows the pg error itself
  return {
    execute: (trx, sql, parameters) => trx.executeQuery(CompiledQuery.raw(sql, parameters)),
  };
}
