import { PostgreSqlContainer, type StartedPostgreSqlContainer } from '@testcontainers/postgresql';
import { Kysely, PostgresDialect, type Transaction } from 'kysely';
import pg from 'pg';
import { afterAll, beforeAll, describe, expect, test } from 'vitest';
import { createSessionFactory } from '@zusammen/core';
import { PostgreSQLOrmPersistence } from '@zusammen/postgresql';
import { kyselyTransactions } from '@zusammen/postgresql/kysely';
import { applicationTransactionContract, InMemoryTransport } from '@zusammen/testing';

interface Database {
  business: { key: string; value: string };
}

let container: StartedPostgreSqlContainer;
let pool: pg.Pool;
let db: Kysely<Database>;
let schema = 0;

beforeAll(async () => {
  container = await new PostgreSqlContainer('postgres:18').start();
  pool = new pg.Pool({ connectionString: container.getConnectionUri() });
  // pool.end() resolves before its connections close; stopping the container then terminates them with an error
  pool.on('error', () => undefined);
  pool.on('connect', (client) => client.on('error', () => undefined));
  // Tests use unique keys, and a schema of their own for the outbox
  await pool.query('CREATE TABLE business (key text PRIMARY KEY, value text NOT NULL)');
  // Kysely shares the pool; destroying it would end the pool, so afterAll ends the pool instead
  db = new Kysely<Database>({ dialect: new PostgresDialect({ pool }) });
}, 120_000);

afterAll(async () => {
  await db.destroy();
  await container.stop();
});

async function setup() {
  const schemaName = `zusammen_kysely_${String(++schema)}`;
  await pool.query(`CREATE SCHEMA ${schemaName}`);
  const persistence = new PostgreSQLOrmPersistence({
    pool,
    schemaName,
    cleanupIntervalMs: 0,
    transactions: kyselyTransactions(db),
  });
  await persistence.connect();
  await persistence.createResources();
  const read = async (key: string) =>
    (await db.selectFrom('business').select('value').where('key', '=', key).executeTakeFirst())?.value;
  return { persistence, read };
}

applicationTransactionContract<Transaction<Database>>('postgresql (Kysely)', {
  setup: async () => {
    const { persistence, read } = await setup();
    return {
      persistence,
      runTransaction: (work) => db.transaction().execute(work),
      write: async (trx, key, value) => {
        await trx.insertInto('business').values({ key, value }).execute();
      },
      read,
    };
  },
});

describe('postgresql with Kysely', () => {
  test('a session runs in the Kysely transaction', async () => {
    const { persistence, read } = await setup();
    const transport = new InMemoryTransport();
    const factory = createSessionFactory({ persistence, transport, maxCommitDurationMs: 15_000 });
    await factory.start();

    const sessionId = await factory.runInTransaction(
      (work) => db.transaction().execute(work),
      async (session) => {
        await session.transactionContext.insertInto('business').values({ key: 'order-1', value: 'placed' }).execute();
        await session.send('billing', { orderId: 'order-1' }, { messageType: 'Sales.PlaceOrder' });
        return session.sessionId;
      },
    );

    expect(await read('order-1')).toBe('placed');
    expect(transport.dispatched).toHaveLength(1);
    expect(await persistence.get(sessionId)).toMatchObject({ dispatched: true });
  });
});
