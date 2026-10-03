import { MongoDBContainer, type StartedMongoDBContainer } from '@testcontainers/mongodb';
import { RabbitMQContainer, type StartedRabbitMQContainer } from '@testcontainers/rabbitmq';
import amqp, { type Channel, type ChannelModel, type ConsumeMessage } from 'amqplib';
import { MongoClient, type ClientSession, type Db } from 'mongodb';
import {
  createSessionFactory,
  type PersistenceProvider,
  type SessionFactory,
  type TransportProvider,
} from '@zusammen/core';
import { MongoDBPersistence } from '@zusammen/mongodb';
import { RabbitMQTransport, ZUSAMMEN_EVENTS_EXCHANGE } from '@zusammen/rabbitmq';

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
  #mongo!: StartedMongoDBContainer;
  #rabbit!: StartedRabbitMQContainer;
  #observer!: ChannelModel;
  #factories: SessionFactory<ClientSession>[] = [];
  client!: MongoClient;
  channel!: Channel;
  amqpUrl!: string;

  async start(): Promise<void> {
    [this.#mongo, this.#rabbit] = await Promise.all([
      new MongoDBContainer('mongo:8').start(),
      new RabbitMQContainer('rabbitmq:4-management').start(),
    ]);
    this.client = new MongoClient(this.#mongo.getConnectionString(), { directConnection: true });
    await this.client.connect();
    this.amqpUrl = this.#rabbit.getAmqpUrl();
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
  async instance(options: { database: Db; controlQueue: string; maxCommitDurationMs?: number }) {
    const faults: Faults = { failingDispatches: 0, failEveryNthDispatch: 0, failCommit: false };
    const persistence = new MongoDBPersistence({ client: this.client, databaseName: options.database.databaseName });
    const transport = new RabbitMQTransport({
      url: this.amqpUrl,
      controlQueue: options.controlQueue,
      recovery: { initialDelay: 100, maxDelay: 500 },
    });

    let dispatches = 0;
    const faultyTransport: TransportProvider = {
      connect: () => transport.connect(),
      disconnect: () => transport.disconnect(),
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
