import amqp, {
  type Channel,
  type ChannelModel,
  type ConfirmChannel,
  type ConsumeMessage,
  type Message,
  type Options,
  type RecoveringChannelModel,
  type RecoveryOptions,
  type SocketOptions,
} from 'amqplib';
import {
  MissingResourcesError,
  silentLogger,
  UnroutableMessageError,
  type ControlMessage,
  type ControlMessageHandler,
  type ControlResult,
  type Logger,
  type MessageConvention,
  type StopControlProcessing,
  type TransportOperation,
  type TransportProvider,
} from '@zusammen/core';
import {
  allControlQueues,
  CONTROL_MESSAGE_TYPE,
  controlQueueNames,
  declareControlQueues,
  decodeControlMessage,
  delayLevelFor,
  encodeControlMessage,
  type ControlQueueNames,
  type QueueType,
} from './control-queues.js';
import { zusammenTopology, type Route, type RoutingTopology } from './topology.js';

export const DEFAULT_CONTROL_QUEUE = 'zusammen.control';
export const ERROR_HEADER = 'zusammen.error';
const CONSUMER_RESTART_DELAY_MS = 1_000;

export interface RabbitMQTransportOptions {
  url: string | Options.Connect;
  socketOptions?: SocketOptions | undefined;
  /** Reconnection settings; reconnection is always on. */
  recovery?: Omit<RecoveryOptions, 'setup' | 'waitForConnect'> | undefined;
  /** Defaults to the Zusammen topology. */
  topology?: RoutingTopology | undefined;
  /** Defaults to {@link DEFAULT_CONTROL_QUEUE}; use one per application sharing a broker. */
  controlQueue?: string | undefined;
  /** Queue type for the control and error queues. Defaults to quorum. */
  queueType?: QueueType | undefined;
  /** Control messages processed concurrently per instance. Defaults to 10. */
  prefetch?: number | undefined;
  logger?: Logger | undefined;
}

export class RabbitMQTransport implements TransportProvider {
  readonly #options: RabbitMQTransportOptions;
  readonly #topology: RoutingTopology;
  readonly #queues: ControlQueueNames;
  readonly #logger: Logger;
  readonly #inFlight = new Set<Promise<void>>();
  // Mandatory publishes the broker returned; the return always arrives before the confirm
  readonly #returned = new Set<string>();

  #connection: RecoveringChannelModel | undefined;
  #model: ChannelModel | undefined;
  #publishChannel: Promise<ConfirmChannel> | undefined;
  #consumer: { channel: Channel; consumerTag: string } | undefined;
  #handler: ControlMessageHandler | undefined;
  #createsResources = false;
  // Optional exchanges known to exist on the current connection
  readonly #knownExchanges = new Set<string>();

  constructor(options: RabbitMQTransportOptions) {
    this.#options = options;
    this.#topology = options.topology ?? zusammenTopology();
    this.#queues = controlQueueNames(options.controlQueue ?? DEFAULT_CONTROL_QUEUE);
    this.#logger = options.logger ?? silentLogger;
  }

  async connect(): Promise<void> {
    if (this.#connection !== undefined) {
      return;
    }
    const connection = await amqp.connect(this.#options.url, {
      ...this.#options.socketOptions,
      recovery: { ...this.#options.recovery, setup: (model: ChannelModel) => this.#setup(model) },
    });
    connection.on('disconnect', (error: Error) => {
      this.#logger.warn('Disconnected from RabbitMQ, reconnecting', { error });
      this.#model = undefined;
      this.#publishChannel = undefined;
      this.#consumer = undefined;
      this.#knownExchanges.clear();
    });
    connection.on('reconnect-failed', (error: Error) => {
      this.#logger.error('Giving up reconnecting to RabbitMQ', { error });
    });
    connection.on('error', (error: Error) => {
      this.#logger.error('RabbitMQ connection error', { error });
    });
    this.#connection = connection;
  }

  async disconnect(): Promise<void> {
    await this.#stopConsuming();
    const connection = this.#connection;
    this.#connection = undefined;
    this.#model = undefined;
    this.#publishChannel = undefined;
    this.#knownExchanges.clear();
    await connection?.close();
  }

  /** Declares the topology's exchanges and the control, error and delay queues. */
  async createResources(): Promise<void> {
    // From now on, optional exchanges (e.g. event exchanges without subscribers yet) are declared on first use
    this.#createsResources = true;
    const channel = await this.#requireModel().createChannel();
    try {
      await this.#topology.createResources(channel);
      await declareControlQueues(channel, this.#queues, this.#options.queueType ?? 'quorum');
    } finally {
      await channel.close().catch(() => undefined);
    }
  }

  async verifyResources(): Promise<void> {
    const missing: string[] = [];
    for (const exchange of this.#topology.requiredExchanges()) {
      if (!(await this.#exists((channel) => channel.checkExchange(exchange)))) {
        missing.push(`RabbitMQ exchange '${exchange}'`);
      }
    }
    for (const queue of allControlQueues(this.#queues)) {
      if (!(await this.#exists((channel) => channel.checkQueue(queue)))) missing.push(`RabbitMQ queue '${queue}'`);
    }
    if (missing.length > 0) {
      throw new MissingResourcesError(missing);
    }
  }

  // A failed passive check closes the channel, so each check gets its own
  async #exists(check: (channel: Channel) => Promise<unknown>): Promise<boolean> {
    const channel = await this.#requireModel().createChannel();
    channel.on('error', () => undefined);
    try {
      await check(channel);
      return true;
    } catch {
      return false;
    } finally {
      await channel.close().catch(() => undefined);
    }
  }

  // Whether to publish to a route whose exchange may not exist; declares it when resource creation is enabled
  async #ensureOptionalExchange(route: Route): Promise<boolean> {
    if (route.optionalExchange === undefined || this.#knownExchanges.has(route.exchange)) {
      return true;
    }
    if (!(await this.#exists((channel) => channel.checkExchange(route.exchange)))) {
      if (!this.#createsResources) {
        this.#logger.debug('Exchange does not exist, nobody subscribed: skipping', { exchange: route.exchange });
        return false;
      }
      const channel = await this.#requireModel().createChannel();
      try {
        await channel.assertExchange(route.exchange, route.optionalExchange.type, { durable: true });
      } finally {
        await channel.close().catch(() => undefined);
      }
    }
    this.#knownExchanges.add(route.exchange);
    return true;
  }

  #requireModel(): ChannelModel {
    if (this.#model === undefined) {
      throw new Error('Not connected to RabbitMQ');
    }
    return this.#model;
  }

  validateConvention(convention: MessageConvention): void {
    this.#topology.validateConvention?.(convention);
  }

  async dispatch(operations: readonly TransportOperation[]): Promise<void> {
    const channel = await this.#getPublishChannel();
    await Promise.all(
      operations.map(async (operation) => {
        const route = this.#topology.route(operation);
        if (!(await this.#ensureOptionalExchange(route))) {
          return;
        }
        const options = this.#topology.publishOptions?.(operation) ?? toPublishOptions(operation);
        await this.#publish(channel, route, Buffer.from(operation.body), options, {
          mandatory: operation.intent === 'send',
          messageId: operation.messageId,
        });
      }),
    );
  }

  async sendControl(message: ControlMessage, delayMs: number): Promise<void> {
    await this.#publishControl(this.#controlQueueFor(delayMs), message);
  }

  #controlQueueFor(delayMs: number): string {
    const level = delayLevelFor(delayMs);
    return level === 0 ? this.#queues.control : this.#queues.delay(level);
  }

  async consumeControl(handler: ControlMessageHandler): Promise<StopControlProcessing> {
    this.#handler = handler;
    if (this.#model !== undefined) {
      await this.#startConsumer(this.#model);
    }
    return () => this.#stopConsuming();
  }

  // Runs after every (re)connect: channels and consumer. Resources are durable and never declared here.
  async #setup(model: ChannelModel): Promise<void> {
    this.#model = model;
    await this.#createPublishChannel(model);
    if (this.#handler !== undefined) {
      await this.#startConsumer(model);
    }
  }

  #createPublishChannel(model: ChannelModel): Promise<ConfirmChannel> {
    const created = model.createConfirmChannel().then((channel) => {
      channel.on('return', (message: Message) => {
        this.#returned.add(returnKey(message.fields.exchange, message.fields.routingKey, message.properties.messageId));
      });
      channel.on('error', (error: Error) => {
        this.#logger.warn('RabbitMQ publish channel error', { error });
      });
      // e.g. publishing to a missing exchange closes the channel; the next publish opens a new one
      channel.on('close', () => {
        if (this.#publishChannel === created) {
          this.#publishChannel = undefined;
        }
      });
      return channel;
    });
    this.#publishChannel = created;
    return created;
  }

  #getPublishChannel(): Promise<ConfirmChannel> {
    if (this.#publishChannel !== undefined) {
      return this.#publishChannel;
    }
    if (this.#model === undefined) {
      return Promise.reject(new Error('Not connected to RabbitMQ'));
    }
    return this.#createPublishChannel(this.#model);
  }

  async #publish(
    channel: ConfirmChannel,
    route: Route,
    content: Buffer,
    options: Options.Publish,
    tracking: { mandatory: boolean; messageId: string },
  ): Promise<void> {
    const key = returnKey(route.exchange, route.routingKey, tracking.messageId);
    await new Promise<void>((resolve, reject) => {
      channel.publish(
        route.exchange,
        route.routingKey,
        content,
        { ...options, mandatory: tracking.mandatory },
        (error) => {
          if (error) {
            reject(error instanceof Error ? error : new Error(String(error)));
          } else {
            resolve();
          }
        },
      );
    });
    if (this.#returned.delete(key)) {
      throw new UnroutableMessageError(tracking.messageId, describeRoute(route));
    }
  }

  async #publishControl(queue: string, message: ControlMessage, headers?: Record<string, string>): Promise<void> {
    const channel = await this.#getPublishChannel();
    await this.#publish(
      channel,
      { exchange: '', routingKey: queue },
      encodeControlMessage(message),
      {
        messageId: message.sessionId,
        type: CONTROL_MESSAGE_TYPE,
        contentType: 'application/json',
        persistent: true,
        ...(headers !== undefined && { headers }),
      },
      { mandatory: true, messageId: message.sessionId },
    );
  }

  async #startConsumer(model: ChannelModel): Promise<void> {
    if (this.#consumer !== undefined) {
      return;
    }
    const channel = await model.createChannel();
    channel.on('error', (error: Error) => {
      this.#logger.warn('RabbitMQ consumer channel error', { error });
    });
    channel.on('close', () => {
      if (this.#consumer?.channel !== channel) {
        return;
      }
      this.#consumer = undefined;
      // Channel-level failure on a live connection (e.g. the queue was deleted): resume consuming, with a delay to
      // avoid a tight loop while the cause persists
      setTimeout(() => {
        if (this.#handler !== undefined && this.#model === model && this.#consumer === undefined) {
          this.#startConsumer(model).catch((error: unknown) => {
            this.#logger.error('Failed to restart the control message consumer', { error });
          });
        }
      }, CONSUMER_RESTART_DELAY_MS).unref();
    });
    await channel.prefetch(this.#options.prefetch ?? 10);
    const { consumerTag } = await channel.consume(this.#queues.control, (message) => {
      if (message !== null) {
        const processing = this.#process(channel, message).finally(() => this.#inFlight.delete(processing));
        this.#inFlight.add(processing);
      }
    });
    this.#consumer = { channel, consumerTag };
  }

  async #stopConsuming(): Promise<void> {
    this.#handler = undefined;
    const consumer = this.#consumer;
    this.#consumer = undefined;
    if (consumer !== undefined) {
      await consumer.channel.cancel(consumer.consumerTag).catch(() => undefined);
    }
    await Promise.allSettled(this.#inFlight);
    await consumer?.channel.close().catch(() => undefined);
  }

  async #process(channel: Channel, delivery: ConsumeMessage): Promise<void> {
    const handler = this.#handler;
    try {
      let message: ControlMessage;
      try {
        message = decodeControlMessage(delivery.content);
      } catch (error) {
        await this.#moveToErrorQueue(delivery, error);
        channel.ack(delivery);
        return;
      }
      if (handler === undefined) {
        channel.nack(delivery, false, true);
        return;
      }
      await this.#apply(await handler(message), message);
      channel.ack(delivery);
    } catch (error) {
      // Redelivered later; processing is idempotent
      this.#logger.warn('Failed to process control message, requeueing', { error });
      try {
        channel.nack(delivery, false, true);
      } catch {
        // Channel closed: the broker redelivers unacknowledged messages
      }
    }
  }

  async #apply(result: ControlResult, message: ControlMessage): Promise<void> {
    switch (result.kind) {
      case 'ack':
        return;
      case 'retry':
        await this.#publishControl(this.#controlQueueFor(result.delayMs), result.next);
        return;
      case 'error':
        await this.#publishControl(this.#queues.error, message, { [ERROR_HEADER]: result.error.message });
        return;
    }
  }

  async #moveToErrorQueue(delivery: ConsumeMessage, error: unknown): Promise<void> {
    const channel = await this.#getPublishChannel();
    const reason = error instanceof Error ? error.message : String(error);
    this.#logger.error('Moving malformed control message to the error queue', { reason });
    await new Promise<void>((resolve, reject) => {
      channel.sendToQueue(
        this.#queues.error,
        delivery.content,
        { ...delivery.properties, headers: { ...delivery.properties.headers, [ERROR_HEADER]: reason } },
        (failure) => {
          if (failure) reject(failure instanceof Error ? failure : new Error(String(failure)));
          else resolve();
        },
      );
    });
  }
}

function toPublishOptions(operation: TransportOperation): Options.Publish {
  const options: Options.Publish = {
    messageId: operation.messageId,
    type: operation.messageType,
    contentType: operation.properties.contentType,
    persistent: true,
    headers: { ...operation.headers },
  };
  if (operation.properties.correlationId !== undefined) options.correlationId = operation.properties.correlationId;
  if (operation.properties.replyTo !== undefined) options.replyTo = operation.properties.replyTo;
  return options;
}

function returnKey(exchange: string, routingKey: string, messageId: unknown): string {
  return `${exchange}\u0000${routingKey}\u0000${String(messageId)}`;
}

function describeRoute(route: Route): string {
  return route.exchange === '' ? route.routingKey : `${route.exchange} (${route.routingKey})`;
}
