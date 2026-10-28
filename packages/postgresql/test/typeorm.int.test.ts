import { PostgreSqlContainer, type StartedPostgreSqlContainer } from '@testcontainers/postgresql';
import pg from 'pg';
import { DataSource, EntitySchema, type EntityManager } from 'typeorm';
import { afterAll, beforeAll, describe, expect, expectTypeOf, test } from 'vitest';
import { createSessionFactory } from '@zusammen/core';
import { PostgreSQLOrmPersistence } from '@zusammen/postgresql';
import { typeormTransactions } from '@zusammen/postgresql/typeorm';
import { applicationTransactionContract, InMemoryTransport } from '@zusammen/testing';

interface Business {
  key: string;
  value: string;
}

const BusinessEntity = new EntitySchema<Business>({
  name: 'Business',
  tableName: 'business',
  columns: { key: { type: 'text', primary: true }, value: { type: 'text' } },
});

let container: StartedPostgreSqlContainer;
let pool: pg.Pool;
let dataSource: DataSource;
let schema = 0;

beforeAll(async () => {
  container = await new PostgreSqlContainer('postgres:18').start();
  pool = new pg.Pool({ connectionString: container.getConnectionUri() });
  // pool.end() resolves before its connections close; stopping the container then terminates them with an error
  pool.on('error', () => undefined);
  pool.on('connect', (client) => client.on('error', () => undefined));
  // Tests use unique keys, and a schema of their own for the outbox
  await pool.query('CREATE TABLE business (key text PRIMARY KEY, value text NOT NULL)');
  dataSource = new DataSource({ type: 'postgres', url: container.getConnectionUri(), entities: [BusinessEntity] });
  await dataSource.initialize();
}, 120_000);

afterAll(async () => {
  await dataSource.destroy();
  await pool.end();
  await container.stop();
});

async function setup() {
  const schemaName = `zusammen_typeorm_${String(++schema)}`;
  await pool.query(`CREATE SCHEMA ${schemaName}`);
  const persistence = new PostgreSQLOrmPersistence({
    pool,
    schemaName,
    cleanupIntervalMs: 0,
    transactions: typeormTransactions(dataSource),
  });
  await persistence.connect();
  await persistence.createResources();
  const read = async (key: string) => (await dataSource.manager.findOneBy(BusinessEntity, { key }))?.value;
  return { persistence, read };
}

applicationTransactionContract<EntityManager>('postgresql (TypeORM)', {
  setup: async () => {
    const { persistence, read } = await setup();
    return {
      persistence,
      runTransaction: (work) => dataSource.transaction(work),
      write: async (manager, key, value) => {
        await manager.insert(BusinessEntity, { key, value });
      },
      read,
    };
  },
});

describe('postgresql with TypeORM', () => {
  test('a session runs in the TypeORM transaction', async () => {
    const { persistence, read } = await setup();
    const transport = new InMemoryTransport();
    const factory = createSessionFactory({ persistence, transport, maxCommitDurationMs: 15_000 });
    await factory.start();

    const sessionId = await factory.runInTransaction(
      (work) => dataSource.transaction(work),
      async (session) => {
        expectTypeOf(session.transactionContext).toEqualTypeOf<EntityManager>();
        await session.transactionContext.insert(BusinessEntity, { key: 'order-1', value: 'placed' });
        await session.send('billing', { orderId: 'order-1' }, { messageType: 'Sales.PlaceOrder' });
        return session.sessionId;
      },
    );

    expect(await read('order-1')).toBe('placed');
    expect(transport.dispatched).toHaveLength(1);
    expect(await persistence.get(sessionId)).toMatchObject({ dispatched: true });
  });
});
