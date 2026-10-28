import { PostgreSqlContainer, type StartedPostgreSqlContainer } from '@testcontainers/postgresql';
import pg from 'pg';
import { DataTypes, Sequelize, type Model, type ModelStatic, type Transaction } from 'sequelize';
import { afterAll, beforeAll, describe, expect, test } from 'vitest';
import { createSessionFactory } from '@zusammen/core';
import { PostgreSQLOrmPersistence } from '@zusammen/postgresql';
import { sequelizeTransactions } from '@zusammen/postgresql/sequelize';
import { applicationTransactionContract, InMemoryTransport } from '@zusammen/testing';

interface Business extends Model {
  key: string;
  value: string;
}

let container: StartedPostgreSqlContainer;
let pool: pg.Pool;
let sequelize: Sequelize;
let business: ModelStatic<Business>;
let schema = 0;

beforeAll(async () => {
  container = await new PostgreSqlContainer('postgres:18').start();
  pool = new pg.Pool({ connectionString: container.getConnectionUri() });
  // pool.end() resolves before its connections close; stopping the container then terminates them with an error
  pool.on('error', () => undefined);
  pool.on('connect', (client) => client.on('error', () => undefined));
  // Tests use unique keys, and a schema of their own for the outbox
  await pool.query('CREATE TABLE business (key text PRIMARY KEY, value text NOT NULL)');
  sequelize = new Sequelize(container.getConnectionUri(), { logging: false });
  business = sequelize.define<Business>(
    'Business',
    { key: { type: DataTypes.TEXT, primaryKey: true }, value: { type: DataTypes.TEXT, allowNull: false } },
    { tableName: 'business', timestamps: false },
  );
}, 120_000);

afterAll(async () => {
  await sequelize.close();
  await pool.end();
  await container.stop();
});

async function setup() {
  const schemaName = `zusammen_sequelize_${String(++schema)}`;
  await pool.query(`CREATE SCHEMA ${schemaName}`);
  const persistence = new PostgreSQLOrmPersistence({
    pool,
    schemaName,
    cleanupIntervalMs: 0,
    transactions: sequelizeTransactions(sequelize),
  });
  await persistence.connect();
  await persistence.createResources();
  const read = async (key: string) => (await business.findByPk(key))?.value;
  return { persistence, read };
}

applicationTransactionContract<Transaction>('postgresql (Sequelize)', {
  setup: async () => {
    const { persistence, read } = await setup();
    return {
      persistence,
      runTransaction: (work) => sequelize.transaction(work),
      write: async (transaction, key, value) => {
        await business.create({ key, value }, { transaction });
      },
      read,
    };
  },
});

describe('postgresql with Sequelize', () => {
  test('a session runs in the Sequelize transaction', async () => {
    const { persistence, read } = await setup();
    const transport = new InMemoryTransport();
    const factory = createSessionFactory({ persistence, transport, maxCommitDurationMs: 15_000 });
    await factory.start();

    const sessionId = await factory.runInTransaction(
      (work) => sequelize.transaction(work),
      async (session) => {
        await business.create({ key: 'order-1', value: 'placed' }, { transaction: session.transactionContext });
        await session.send('billing', { orderId: 'order-1' }, { messageType: 'Sales.PlaceOrder' });
        return session.sessionId;
      },
    );

    expect(await read('order-1')).toBe('placed');
    expect(transport.dispatched).toHaveLength(1);
    expect(await persistence.get(sessionId)).toMatchObject({ dispatched: true });
  });
});
