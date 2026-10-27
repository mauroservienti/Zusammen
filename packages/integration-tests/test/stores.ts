import { createServer } from 'node:net';
import { PostgreSqlContainer, type StartedPostgreSqlContainer } from '@testcontainers/postgresql';
import { GenericContainer, Wait, type StartedTestContainer } from 'testcontainers';
import { MongoClient, type ClientSession } from 'mongodb';
import pg, { type PoolClient } from 'pg';
import type { PersistenceProvider } from '@zusammen/core';
import { MongoDBPersistence } from '@zusammen/mongodb';
import { PostgreSQLPersistence } from '@zusammen/postgresql';
import { unique, waitFor } from './util.js';

/** A database (or schema) of its own for one test, with an `orders` table/collection for business data. */
export interface StoreDatabase<TContext> {
  /** A new provider on this database, as each application instance has its own. */
  createPersistence(): PersistenceProvider<TContext>;
  insertOrder(context: TContext, orderId: string): Promise<void>;
  hasOrder(orderId: string): Promise<boolean>;
  countOrders(): Promise<number>;
  /** Outbox records, optionally only those (not) dispatched. */
  countOutbox(filter?: { dispatched: boolean }): Promise<number>;
}

/** A database server the end-to-end scenarios run against. */
export interface Store<TContext> {
  readonly name: string;
  start(): Promise<void>;
  stop(): Promise<void>;
  createDatabase(): Promise<StoreDatabase<TContext>>;
}

export class MongoDBStore implements Store<ClientSession> {
  readonly name = 'MongoDB';
  #container!: StartedTestContainer;
  connectionString!: string;
  client!: MongoClient;

  async start(): Promise<void> {
    let port: number;
    ({ container: this.#container, port } = await startReplicaSet());
    this.connectionString = `mongodb://localhost:${String(port)}/?replicaSet=rs0`;
    this.client = new MongoClient(this.connectionString);
    await this.client.connect();
  }

  async stop(): Promise<void> {
    await this.client.close();
    await this.#container.stop();
  }

  async createDatabase(): Promise<StoreDatabase<ClientSession>> {
    const database = this.client.db(unique('zusammen').replaceAll('.', '_'));
    // Concurrent transactions can't implicitly create the same collection
    await database.createCollection('orders');
    const orders = database.collection<{ _id: string }>('orders');
    const outbox = new MongoDBPersistence({ client: this.client, databaseName: database.databaseName }).collection;
    return {
      createPersistence: () => new MongoDBPersistence({ client: this.client, databaseName: database.databaseName }),
      insertOrder: async (session, orderId) => {
        await orders.insertOne({ _id: orderId }, { session });
      },
      hasOrder: async (orderId) => (await orders.findOne({ _id: orderId })) !== null,
      countOrders: () => orders.countDocuments(),
      countOutbox: (filter) => outbox.countDocuments(filter === undefined ? {} : { Dispatched: filter.dispatched }),
    };
  }
}

export class PostgreSQLStore implements Store<PoolClient> {
  readonly name = 'PostgreSQL';
  #container!: StartedPostgreSqlContainer;
  #pool!: pg.Pool;

  async start(): Promise<void> {
    this.#container = await new PostgreSqlContainer('postgres:18').start();
    this.#pool = new pg.Pool({ connectionString: this.#container.getConnectionUri(), max: 50 });
  }

  async stop(): Promise<void> {
    await this.#pool.end();
    await this.#container.stop();
  }

  async createDatabase(): Promise<StoreDatabase<PoolClient>> {
    const schemaName = unique('zusammen').replaceAll('.', '_');
    await this.#pool.query(`CREATE SCHEMA ${schemaName}`);
    await this.#pool.query(`CREATE TABLE ${schemaName}.orders (id text PRIMARY KEY)`);
    const outbox = `${schemaName}.zusammen_outbox`;
    return {
      createPersistence: () => new PostgreSQLPersistence({ pool: this.#pool, schemaName, cleanupIntervalMs: 0 }),
      insertOrder: async (client, orderId) => {
        await client.query(`INSERT INTO ${schemaName}.orders (id) VALUES ($1)`, [orderId]);
      },
      hasOrder: async (orderId) =>
        (await this.#pool.query(`SELECT 1 FROM ${schemaName}.orders WHERE id = $1`, [orderId])).rowCount === 1,
      countOrders: () => this.#count(`SELECT count(*) FROM ${schemaName}.orders`),
      countOutbox: (filter) =>
        filter === undefined
          ? this.#count(`SELECT count(*) FROM ${outbox}`)
          : this.#count(`SELECT count(*) FROM ${outbox} WHERE dispatched = $1`, [filter.dispatched]),
    };
  }

  async #count(query: string, values: unknown[] = []): Promise<number> {
    const { rows } = await this.#pool.query<{ count: string }>(query, values);
    return Number(rows[0]?.count);
  }
}

/**
 * A single-node replica set reachable from the host as a replica set (not only through a direct connection): the
 * container listens on the same port it's mapped to, and the member is registered as localhost:<port>. Drivers that
 * report direct connections as standalone servers (e.g. .NET) then support transactions too.
 */
async function startReplicaSet(): Promise<{ container: StartedTestContainer; port: number }> {
  const port = await freePort();
  const container = await new GenericContainer('mongo:8')
    .withCommand(['--replSet', 'rs0', '--bind_ip_all', '--port', String(port)])
    .withExposedPorts({ container: port, host: port })
    .withWaitStrategy(Wait.forLogMessage(/Waiting for connections/))
    .start();
  const initiate = `rs.initiate({ _id: 'rs0', members: [{ _id: 0, host: 'localhost:${String(port)}' }] })`;
  await container.exec(['mongosh', '--quiet', '--port', String(port), '--eval', initiate]);
  await waitFor(async () => {
    const status = await container.exec([
      'mongosh',
      '--quiet',
      '--port',
      String(port),
      '--eval',
      'db.hello().isWritablePrimary',
    ]);
    return status.output.trim() === 'true';
  }, 60_000);
  return { container, port };
}

function freePort(): Promise<number> {
  return new Promise((resolve, reject) => {
    const server = createServer();
    server.once('error', reject);
    server.listen(0, () => {
      const address = server.address();
      server.close(() => {
        if (typeof address === 'object' && address !== null) resolve(address.port);
        else reject(new Error('No port'));
      });
    });
  });
}
