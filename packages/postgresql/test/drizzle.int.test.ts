import { PostgreSqlContainer, type StartedPostgreSqlContainer } from '@testcontainers/postgresql';
import { eq } from 'drizzle-orm';
import { drizzle, type NodePgDatabase } from 'drizzle-orm/node-postgres';
import { pgSchema, text } from 'drizzle-orm/pg-core';
import pg from 'pg';
import { afterAll, beforeAll, describe, expect, test } from 'vitest';
import { createSessionFactory } from '@zusammen/core';
import { PostgreSQLOrmPersistence } from '@zusammen/postgresql';
import { drizzleTransactions, type DrizzleTransaction } from '@zusammen/postgresql/drizzle';
import { applicationTransactionContract, InMemoryTransport } from '@zusammen/testing';

let container: StartedPostgreSqlContainer;
let pool: pg.Pool;
let db: NodePgDatabase;
let schema = 0;

beforeAll(async () => {
  container = await new PostgreSqlContainer('postgres:18').start();
  pool = new pg.Pool({ connectionString: container.getConnectionUri() });
  // pool.end() resolves before its connections close; stopping the container then terminates them with an error
  pool.on('error', () => undefined);
  pool.on('connect', (client) => client.on('error', () => undefined));
  db = drizzle(pool);
}, 120_000);

afterAll(async () => {
  await pool.end();
  await container.stop();
});

/** A fresh schema with a business table and the outbox, so tests don't see each other's data. */
async function setup() {
  const schemaName = `zusammen_drizzle_${String(++schema)}`;
  await pool.query(`CREATE SCHEMA ${schemaName}`);
  await pool.query(`CREATE TABLE ${schemaName}.business (key text PRIMARY KEY, value text NOT NULL)`);
  const business = pgSchema(schemaName).table('business', { key: text().primaryKey(), value: text().notNull() });
  const persistence = new PostgreSQLOrmPersistence({
    pool,
    schemaName,
    cleanupIntervalMs: 0,
    transactions: drizzleTransactions(db),
  });
  await persistence.connect();
  await persistence.createResources();
  const read = async (key: string) => (await db.select().from(business).where(eq(business.key, key)))[0]?.value;
  return { persistence, business, read };
}

applicationTransactionContract<DrizzleTransaction<NodePgDatabase>>('postgresql (Drizzle)', {
  setup: async () => {
    const { persistence, business, read } = await setup();
    return {
      persistence,
      runTransaction: (work) => db.transaction(work),
      write: async (tx, key, value) => {
        await tx.insert(business).values({ key, value });
      },
      read,
    };
  },
});

describe('postgresql with Drizzle', () => {
  test('a session runs in the Drizzle transaction', async () => {
    const { persistence, business, read } = await setup();
    const transport = new InMemoryTransport();
    const factory = createSessionFactory({ persistence, transport, maxCommitDurationMs: 15_000 });
    await factory.start();

    const sessionId = await factory.runInTransaction(
      (work) => db.transaction(work),
      async (session) => {
        await session.transactionContext.insert(business).values({ key: 'order-1', value: 'placed' });
        await session.send('billing', { orderId: 'order-1' }, { messageType: 'Sales.PlaceOrder' });
        return session.sessionId;
      },
    );

    expect(await read('order-1')).toBe('placed');
    expect(transport.dispatched).toHaveLength(1);
    expect(await persistence.get(sessionId)).toMatchObject({ dispatched: true });
  });
});
