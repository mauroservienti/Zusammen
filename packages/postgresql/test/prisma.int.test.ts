import { PostgreSqlContainer, type StartedPostgreSqlContainer } from '@testcontainers/postgresql';
import { PrismaPg } from '@prisma/adapter-pg';
import pg from 'pg';
import { afterAll, beforeAll, describe, expect, expectTypeOf, test } from 'vitest';
import { createSessionFactory } from '@zusammen/core';
import { PostgreSQLOrmPersistence } from '@zusammen/postgresql';
import { prismaTransactions, type PrismaTransaction } from '@zusammen/postgresql/prisma';
import { applicationTransactionContract, InMemoryTransport } from '@zusammen/testing';
import { PrismaClient } from './prisma/generated/client.js';

let container: StartedPostgreSqlContainer;
let pool: pg.Pool;
let prisma: PrismaClient;
let schema = 0;

beforeAll(async () => {
  container = await new PostgreSqlContainer('postgres:18').start();
  pool = new pg.Pool({ connectionString: container.getConnectionUri() });
  // pool.end() resolves before its connections close; stopping the container then terminates them with an error
  pool.on('error', () => undefined);
  pool.on('connect', (client) => client.on('error', () => undefined));
  prisma = new PrismaClient({ adapter: new PrismaPg(pool) });
  // The business table (test/prisma/schema.prisma); tests use unique keys, and a schema of their own for the outbox
  await pool.query('CREATE TABLE business (key text PRIMARY KEY, value text NOT NULL)');
}, 120_000);

afterAll(async () => {
  await prisma.$disconnect();
  await pool.end();
  await container.stop();
});

async function setup() {
  const schemaName = `zusammen_prisma_${String(++schema)}`;
  await pool.query(`CREATE SCHEMA ${schemaName}`);
  const persistence = new PostgreSQLOrmPersistence({
    pool,
    schemaName,
    cleanupIntervalMs: 0,
    transactions: prismaTransactions(prisma),
  });
  await persistence.connect();
  await persistence.createResources();
  const read = async (key: string) => (await prisma.business.findUnique({ where: { key } }))?.value;
  return { persistence, read };
}

applicationTransactionContract<PrismaTransaction<PrismaClient>>('postgresql (Prisma)', {
  setup: async () => {
    const { persistence, read } = await setup();
    return {
      persistence,
      runTransaction: (work) => prisma.$transaction(work),
      write: async (tx, key, value) => {
        await tx.business.create({ data: { key, value } });
      },
      read,
    };
  },
});

describe('postgresql with Prisma', () => {
  test('a session runs in the Prisma interactive transaction', async () => {
    const { persistence, read } = await setup();
    const transport = new InMemoryTransport();
    const factory = createSessionFactory({ persistence, transport, maxCommitDurationMs: 15_000 });
    await factory.start();

    const sessionId = await factory.runInTransaction(
      (work) => prisma.$transaction(work),
      async (session) => {
        await session.transactionContext.business.create({ data: { key: 'order-1', value: 'placed' } });
        await session.send('billing', { orderId: 'order-1' }, { messageType: 'Sales.PlaceOrder' });
        return session.sessionId;
      },
    );

    expectTypeOf(sessionId).toEqualTypeOf<string>();
    expect(await read('order-1')).toBe('placed');
    expect(transport.dispatched).toHaveLength(1);
    expect(await persistence.get(sessionId)).toMatchObject({ dispatched: true });
  });
});
