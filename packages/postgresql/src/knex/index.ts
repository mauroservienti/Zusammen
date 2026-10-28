import type { Knex } from 'knex';
import type { ApplicationTransactions } from '../persistence.js';

/**
 * Runs the outbox insert in Knex transactions (`knex.transaction(fn)`), for `PostgreSQLOrmPersistence`:
 * `new PostgreSQLOrmPersistence({ pool, transactions: knexTransactions() })`.
 */
export function knexTransactions(): ApplicationTransactions<Knex.Transaction> {
  // Knex rethrows the pg error itself, where PostgreSQLOrmPersistence looks for it
  return {
    execute: async (trx, sql, parameters) => {
      const { statement, bindings } = toKnexBindings(sql, parameters);
      await trx.raw(statement, bindings as Knex.RawBinding[]);
    },
  };
}

// "... VALUES ($1, $2)" → "... VALUES (?, ?)" with the parameters in order of appearance: Knex binds `?` positionally
function toKnexBindings(sql: string, parameters: unknown[]): { statement: string; bindings: unknown[] } {
  const bindings: unknown[] = [];
  const statement = sql.replace(/\$(\d+)/g, (_, index: string) => {
    bindings.push(parameters[Number(index) - 1]);
    return '?';
  });
  return { statement, bindings };
}
