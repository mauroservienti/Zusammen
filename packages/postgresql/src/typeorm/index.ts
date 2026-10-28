import type { ApplicationTransactions } from '../persistence.js';

interface TypeOrmExecutor {
  query(query: string, parameters?: unknown[]): Promise<unknown>;
}

/** A TypeORM `DataSource`: `dataSource.transaction(fn)` passes an `EntityManager` like `dataSource.manager` to `fn`. */
export interface TypeOrmDataSource<TManager> {
  readonly manager: TManager;
}

/**
 * Runs the outbox insert in TypeORM transactions, for `PostgreSQLOrmPersistence`. `dataSource` only gives the entity
 * manager type: `new PostgreSQLOrmPersistence({ pool, transactions: typeormTransactions(dataSource) })`.
 */
export function typeormTransactions<TManager extends TypeOrmExecutor>(
  dataSource: TypeOrmDataSource<TManager>,
): ApplicationTransactions<TManager>;
export function typeormTransactions(): ApplicationTransactions<TypeOrmExecutor> {
  // TypeORM keeps the pg error in QueryFailedError.driverError, where PostgreSQLOrmPersistence looks for it
  return { execute: (manager, sql, parameters) => manager.query(sql, parameters) };
}
