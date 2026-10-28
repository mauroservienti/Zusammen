import { PostgreSqlContainer, type StartedPostgreSqlContainer } from '@testcontainers/postgresql';
import pg, { type PoolClient } from 'pg';
import { afterAll, beforeAll, describe, expect, test } from 'vitest';
import { createSessionFactory, SessionCommitConflictError, ZusammenError } from '@zusammen/core';
import { PostgreSQLOrmPersistence, PostgreSQLPersistence, type ApplicationTransactions } from '@zusammen/postgresql';
import { applicationTransactionContract, InMemoryTransport } from '@zusammen/testing';

let container: StartedPostgreSqlContainer;
let pool: pg.Pool;
let schema = 0;

beforeAll(async () => {
  container = await new PostgreSqlContainer('postgres:18').start();
  pool = new pg.Pool({ connectionString: container.getConnectionUri() });
  // pool.end() resolves before its connections close; stopping the container then terminates them with an error
  pool.on('error', () => undefined);
  pool.on('connect', (client) => client.on('error', () => undefined));
}, 120_000);

afterAll(async () => {
  await pool.end();
  await container.stop();
});

/** Stands in for an ORM's transaction object: it wraps the connection, and wraps the driver's errors. */
interface FakeOrmTransaction {
  run(sql: string, parameters?: unknown[]): Promise<unknown>;
}

class FakeOrmError extends Error {
  // Where TypeORM keeps the driver's error
  constructor(readonly driverError: unknown) {
    super('Query failed');
  }
}

const fakeOrmTransactions: ApplicationTransactions<FakeOrmTransaction> = {
  execute: (tx, sql, parameters) => tx.run(sql, parameters),
};

/** The ORM's callback-style transaction API, e.g. `prisma.$transaction(fn)`. */
async function fakeOrmTransaction<T>(work: (tx: FakeOrmTransaction) => Promise<T>): Promise<T> {
  return withPgTransaction((client) =>
    work({
      run: async (sql, parameters) => {
        try {
          return await client.query(sql, parameters);
        } catch (error) {
          throw new FakeOrmError(new Error('Driver adapter error', { cause: error }));
        }
      },
    }),
  );
}

/** The application running its own pg transactions, with the session joining them. */
async function withPgTransaction<T>(work: (client: PoolClient) => Promise<T>): Promise<T> {
  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    const result = await work(client);
    await client.query('COMMIT');
    return result;
  } catch (error) {
    await client.query('ROLLBACK');
    throw error;
  } finally {
    client.release();
  }
}

/** A fresh schema with a business table, so tests don't see each other's data. */
async function createSchema() {
  const schemaName = `zusammen_orm_${String(++schema)}`;
  await pool.query(`CREATE SCHEMA ${schemaName}`);
  await pool.query(`CREATE TABLE ${schemaName}.business (key text PRIMARY KEY, value text NOT NULL)`);
  const read = async (key: string) => {
    const { rows } = await pool.query<{ value: string }>(`SELECT value FROM ${schemaName}.business WHERE key = $1`, [
      key,
    ]);
    return rows[0]?.value;
  };
  return { schemaName, read };
}

async function createOrmPersistence() {
  const { schemaName, read } = await createSchema();
  const persistence = new PostgreSQLOrmPersistence({
    pool,
    schemaName,
    cleanupIntervalMs: 0,
    transactions: fakeOrmTransactions,
  });
  await persistence.connect();
  await persistence.createResources();
  return { persistence, schemaName, read };
}

applicationTransactionContract<FakeOrmTransaction>('postgresql (ORM)', {
  setup: async () => {
    const { persistence, schemaName, read } = await createOrmPersistence();
    return {
      persistence,
      runTransaction: fakeOrmTransaction,
      write: async (tx, key, value) => {
        await tx.run(`INSERT INTO ${schemaName}.business (key, value) VALUES ($1, $2)`, [key, value]);
      },
      read,
    };
  },
});

applicationTransactionContract<PoolClient>('postgresql (application pg transactions)', {
  setup: async () => {
    const { schemaName, read } = await createSchema();
    const persistence = new PostgreSQLPersistence({ pool, schemaName, cleanupIntervalMs: 0 });
    await persistence.connect();
    await persistence.createResources();
    return {
      persistence,
      runTransaction: withPgTransaction,
      write: async (client, key, value) => {
        await client.query(`INSERT INTO ${schemaName}.business (key, value) VALUES ($1, $2)`, [key, value]);
      },
      read,
    };
  },
});

describe('postgresql ORM persistence', () => {
  test('sessions run in the ORM transaction with runInTransaction; open() is not available', async () => {
    const { persistence, schemaName, read } = await createOrmPersistence();
    const transport = new InMemoryTransport();
    const factory = createSessionFactory({ persistence, transport, maxCommitDurationMs: 15_000 });
    await factory.start();

    await expect(factory.open()).rejects.toBeInstanceOf(ZusammenError);
    const sessionId = await factory.runInTransaction(fakeOrmTransaction, async (session) => {
      await session.transactionContext.run(`INSERT INTO ${schemaName}.business (key, value) VALUES ($1, $2)`, [
        'order-1',
        'placed',
      ]);
      await session.send('billing', { orderId: 'order-1' }, { messageType: 'Sales.PlaceOrder' });
      return session.sessionId;
    });

    expect(await read('order-1')).toBe('placed');
    expect(transport.dispatched).toHaveLength(1);
    expect(await persistence.get(sessionId)).toMatchObject({ dispatched: true });
  });

  test('finds the unique violation in the error the ORM wraps the driver error in', async () => {
    const { persistence } = await createOrmPersistence();
    await persistence.storeTombstone('session-1');

    const run = fakeOrmTransaction((tx) =>
      persistence.storeOutbox({ id: 'session-1', dispatched: false, transportOperations: [] }, tx),
    );

    const error: unknown = await run.catch((caught: unknown) => caught);
    expect(error).toBeInstanceOf(SessionCommitConflictError);
    expect((error as Error).cause).toBeInstanceOf(FakeOrmError);
  });

  test('other ORM errors are rethrown as they are', async () => {
    const { persistence } = await createOrmPersistence();
    await pool.query(`DROP TABLE ${persistence.table}`);

    const run = fakeOrmTransaction((tx) =>
      persistence.storeOutbox({ id: 'session-1', dispatched: false, transportOperations: [] }, tx),
    );

    await expect(run).rejects.toBeInstanceOf(FakeOrmError);
  });
});
