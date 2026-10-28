import type { Sequelize, Transaction } from 'sequelize';
import type { ApplicationTransactions } from '../persistence.js';

/**
 * Runs the outbox insert in Sequelize managed transactions (`sequelize.transaction(fn)`), for
 * `PostgreSQLOrmPersistence`: `new PostgreSQLOrmPersistence({ pool, transactions: sequelizeTransactions(sequelize) })`.
 */
export function sequelizeTransactions(sequelize: Sequelize): ApplicationTransactions<Transaction> {
  // Bind parameters are `$1`-style, like the outbox's statements. Sequelize keeps the pg error in `parent`, where
  // PostgreSQLOrmPersistence looks for it.
  return {
    execute: (transaction, sql, parameters) => sequelize.query(sql, { bind: parameters, transaction }),
  };
}
