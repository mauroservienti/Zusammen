import { PostgreSqlContainer, type StartedPostgreSqlContainer } from '@testcontainers/postgresql';
import knexFactory, { type Knex } from 'knex';
import pg from 'pg';
import { afterAll, beforeAll, describe, expect, test } from 'vitest';
import { createSessionFactory } from '@zusammen/core';
import { PostgreSQLOrmPersistence } from '@zusammen/postgresql';
import { knexTransactions } from '@zusammen/postgresql/knex';
import { applicationTransactionContract, InMemoryTransport } from '@zusammen/testing';

let container: StartedPostgreSqlContainer;
let pool: pg.Pool;
let knex: Knex;
let schema = 0;

beforeAll(async () => {
  container = await new PostgreSqlContainer('postgres:18').start();
  pool = new pg.Pool({ connectionString: container.getConnectionUri() });
  // pool.end() resolves before its connections close; stopping the container then terminates them with an error
  pool.on('error', () => undefined);
  pool.on('connect', (client) => client.on('error', () => undefined));
  // Tests use unique keys, and a schema of their own for the outbox
  await pool.query('CREATE TABLE business (key text PRIMARY KEY, value text NOT NULL)');
  knex = knexFactory({ client: 'pg', connection: container.getConnectionUri() });
}, 120_000);

afterAll(async () => {
  await knex.destroy();
  await pool.end();
  await container.stop();
});

async function setup() {
  const schemaName = `zusammen_knex_${String(++schema)}`;
  await pool.query(`CREATE SCHEMA ${schemaName}`);
  const persistence = new PostgreSQLOrmPersistence({
    pool,
    schemaName,
    cleanupIntervalMs: 0,
    transactions: knexTransactions(),
  });
  await persistence.connect();
  await persistence.createResources();
  const read = async (key: string) =>
    (await knex<{ key: string; value: string }>('business').where({ key }).first())?.value;
  return { persistence, read };
}

applicationTransactionContract<Knex.Transaction>('postgresql (Knex)', {
  setup: async () => {
    const { persistence, read } = await setup();
    return {
      persistence,
      runTransaction: (work) => knex.transaction(work),
      write: async (trx, key, value) => {
        await trx('business').insert({ key, value });
      },
      read,
    };
  },
});

describe('postgresql with Knex', () => {
  test('a session runs in the Knex transaction', async () => {
    const { persistence, read } = await setup();
    const transport = new InMemoryTransport();
    const factory = createSessionFactory({ persistence, transport, maxCommitDurationMs: 15_000 });
    await factory.start();

    const sessionId = await factory.runInTransaction(
      (work) => knex.transaction(work),
      async (session) => {
        await session.transactionContext('business').insert({ key: 'order-1', value: 'placed' });
        await session.send('billing', { orderId: 'order-1' }, { messageType: 'Sales.PlaceOrder' });
        return session.sessionId;
      },
    );

    expect(await read('order-1')).toBe('placed');
    expect(transport.dispatched).toHaveLength(1);
    expect(await persistence.get(sessionId)).toMatchObject({ dispatched: true });
  });
});
