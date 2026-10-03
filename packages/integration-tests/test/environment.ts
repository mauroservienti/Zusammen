import { createServer } from 'node:net';
import { RabbitMQContainer, type StartedRabbitMQContainer } from '@testcontainers/rabbitmq';
import { GenericContainer, Wait, type StartedTestContainer } from 'testcontainers';
import amqp, { type Channel, type ChannelModel, type ConsumeMessage } from 'amqplib';
import { MongoClient, type ClientSession, type Db } from 'mongodb';
import {
  createSessionFactory,
  type MessageConvention,
  type PersistenceProvider,
  type SessionFactory,
  type TransportProvider,
} from '@zusammen/core';
import { MongoDBPersistence } from '@zusammen/mongodb';
import { RabbitMQTransport, ZUSAMMEN_EVENTS_EXCHANGE, type RoutingTopology } from '@zusammen/rabbitmq';

let sequence = 0;
export const unique = (prefix: string) => `${prefix}.${String(Date.now())}.${String(++sequence)}`;
export const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

export async function waitFor(probe: () => Promise<boolean> | boolean, timeoutMs = 20_000): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (!(await probe())) {
    if (Date.now() > deadline) throw new Error('Timed out waiting');
    await sleep(50);
  }
}

/** Faults injected around the real providers. */
export interface Faults {
  /** Number of upcoming dispatches that fail, e.g. a crash between commit and dispatch. */
  failingDispatches: number;
  /** Fail every n-th dispatch (0: never). */
  failEveryNthDispatch: number;
  beforeStoreOutbox?: (() => Promise<void>) | undefined;
  beforeCommit?: (() => Promise<void>) | undefined;
  /** The commit fails after the control message was sent, like a crash before commit. */
  failCommit: boolean;
}

export class Environment {
  #mongo!: StartedTestContainer;
  mongoConnectionString!: string;
  #rabbit!: StartedRabbitMQContainer;
  #observer!: ChannelModel;
  #factories: SessionFactory<ClientSession>[] = [];
  client!: MongoClient;
  channel!: Channel;
  amqpUrl!: string;
  /** RabbitMQ management API, with the default guest credentials. */
  managementUrl!: string;

  async start(): Promise<void> {
    let mongoPort: number;
    [{ container: this.#mongo, port: mongoPort }, this.#rabbit] = await Promise.all([
      startReplicaSet(),
      new RabbitMQContainer('rabbitmq:4-management').start(),
    ]);
    this.mongoConnectionString = `mongodb://localhost:${String(mongoPort)}/?replicaSet=rs0`;
    this.client = new MongoClient(this.mongoConnectionString);
    await this.client.connect();
    this.amqpUrl = this.#rabbit.getAmqpUrl();
    this.managementUrl = `http://guest:guest@${this.#rabbit.getHost()}:${String(this.#rabbit.getMappedPort(15672))}`;
    this.#observer = await amqp.connect(this.amqpUrl);
    this.channel = await this.#observer.createChannel();
  }

  async stopFactories(): Promise<void> {
    await Promise.all(this.#factories.splice(0).map((factory) => factory.stop()));
  }

  async stop(): Promise<void> {
    await this.stopFactories();
    await this.#observer.close();
    await this.client.close();
    await Promise.all([this.#mongo.stop(), this.#rabbit.stop()]);
  }

  /** An application instance: a started factory on real providers, with fault injection. */
  async instance(options: {
    database: Db;
    controlQueue: string;
    maxCommitDurationMs?: number;
    convention?: MessageConvention;
    topology?: RoutingTopology;
  }) {
    const faults: Faults = { failingDispatches: 0, failEveryNthDispatch: 0, failCommit: false };
    const persistence = new MongoDBPersistence({ client: this.client, databaseName: options.database.databaseName });
    const transport = new RabbitMQTransport({
      url: this.amqpUrl,
      controlQueue: options.controlQueue,
      recovery: { initialDelay: 100, maxDelay: 500 },
      ...(options.topology !== undefined && { topology: options.topology }),
    });

    let dispatches = 0;
    const faultyTransport: TransportProvider = {
      connect: () => transport.connect(),
      disconnect: () => transport.disconnect(),
      createResources: () => transport.createResources(),
      verifyResources: () => transport.verifyResources(),
      validateConvention: (convention) => {
        transport.validateConvention(convention);
      },
      sendControl: (message, delayMs) => transport.sendControl(message, delayMs),
      consumeControl: (handler) => transport.consumeControl(handler),
      dispatch: (operations) => {
        dispatches++;
        if (
          faults.failingDispatches > 0 ||
          (faults.failEveryNthDispatch > 0 && dispatches % faults.failEveryNthDispatch === 0)
        ) {
          faults.failingDispatches = Math.max(0, faults.failingDispatches - 1);
          return Promise.reject(new Error('injected dispatch failure'));
        }
        return transport.dispatch(operations);
      },
    };
    const faultyPersistence: PersistenceProvider<ClientSession> = {
      connect: () => persistence.connect(),
      disconnect: () => persistence.disconnect(),
      createResources: () => persistence.createResources(),
      verifyResources: () => persistence.verifyResources(),
      begin: () => persistence.begin(),
      storeOutbox: async (record, session) => {
        await faults.beforeStoreOutbox?.();
        await persistence.storeOutbox(record, session);
      },
      commit: async (session) => {
        await faults.beforeCommit?.();
        if (faults.failCommit) {
          throw new Error('injected crash before commit');
        }
        await persistence.commit(session);
      },
      rollback: (session) => persistence.rollback(session),
      get: (sessionId) => persistence.get(sessionId),
      markDispatched: (sessionId) => persistence.markDispatched(sessionId),
      storeTombstone: (sessionId) => persistence.storeTombstone(sessionId),
    };

    const factory = createSessionFactory({
      persistence: faultyPersistence,
      transport: faultyTransport,
      createResources: true,
      ...(options.convention !== undefined && { convention: options.convention }),
      maxCommitDurationMs: options.maxCommitDurationMs ?? 3_000,
      controlTiming: {
        initialCommitDelayIncrementMs: 500,
        maxCommitDelayIncrementMs: 1_000,
        initialFailureDelayMs: 500,
      },
    });
    await factory.start();
    this.#factories.push(factory);
    return { factory, faults, outbox: persistence.collection };
  }

  /** A queue receiving events published with the given topic. */
  async subscribe(topic: string) {
    const queue = unique('subscriber');
    const received: ConsumeMessage[] = [];
    await this.channel.assertQueue(queue, { autoDelete: true });
    await this.channel.bindQueue(queue, ZUSAMMEN_EVENTS_EXCHANGE, topic);
    await this.channel.consume(
      queue,
      (message) => {
        if (message !== null) received.push(message);
      },
      { noAck: true },
    );
    return {
      received,
      bodies: () => new Set(received.map((message) => message.content.toString())),
    };
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
