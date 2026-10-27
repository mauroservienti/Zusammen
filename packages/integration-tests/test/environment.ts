import { RabbitMQContainer, type StartedRabbitMQContainer } from '@testcontainers/rabbitmq';
import amqp, { type Channel, type ChannelModel, type ConsumeMessage } from 'amqplib';
import {
  createSessionFactory,
  type MessageConvention,
  type PersistenceProvider,
  type SessionFactory,
  type TransportProvider,
} from '@zusammen/core';
import { RabbitMQTransport, ZUSAMMEN_EVENTS_EXCHANGE, type RoutingTopology } from '@zusammen/rabbitmq';
import type { Store, StoreDatabase } from './stores.js';
import { unique } from './util.js';

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

/** RabbitMQ and a database server, shared by a test file. */
export class Environment<TContext> {
  #rabbit!: StartedRabbitMQContainer;
  #observer!: ChannelModel;
  #factories: SessionFactory<TContext>[] = [];
  channel!: Channel;
  amqpUrl!: string;
  /** RabbitMQ management API, with the default guest credentials. */
  managementUrl!: string;

  constructor(readonly store: Store<TContext>) {}

  async start(): Promise<void> {
    [, this.#rabbit] = await Promise.all([this.store.start(), new RabbitMQContainer('rabbitmq:4-management').start()]);
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
    await Promise.all([this.store.stop(), this.#rabbit.stop()]);
  }

  /** An application instance: a started factory on real providers, with fault injection. */
  async instance(options: {
    database: StoreDatabase<TContext>;
    controlQueue: string;
    maxCommitDurationMs?: number;
    convention?: MessageConvention;
    topology?: RoutingTopology;
  }) {
    const faults: Faults = { failingDispatches: 0, failEveryNthDispatch: 0, failCommit: false };
    const persistence = options.database.createPersistence();
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
    const faultyPersistence: PersistenceProvider<TContext> = {
      connect: () => persistence.connect(),
      disconnect: () => persistence.disconnect(),
      createResources: () => persistence.createResources?.() ?? Promise.resolve(),
      verifyResources: () => persistence.verifyResources?.() ?? Promise.resolve(),
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
    return { factory, faults, persistence };
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
