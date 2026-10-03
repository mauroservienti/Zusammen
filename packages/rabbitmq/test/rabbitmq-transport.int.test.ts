import { RabbitMQContainer, type StartedRabbitMQContainer } from '@testcontainers/rabbitmq';
import amqp, { type Channel, type ChannelModel, type GetMessage } from 'amqplib';
import { afterAll, afterEach, beforeAll, describe, expect, test } from 'vitest';
import {
  createSessionFactory,
  UnroutableMessageError,
  type ControlMessage,
  type ControlResult,
  type TransportOperation,
  type TransportProvider,
} from '@zusammen/core';
import { ERROR_HEADER, RabbitMQTransport, ZUSAMMEN_EVENTS_EXCHANGE } from '@zusammen/rabbitmq';
import { InMemoryPersistence } from '@zusammen/testing';

let container: StartedRabbitMQContainer;
let url: string;
let observer: ChannelModel;
let channel: Channel;
const transports: RabbitMQTransport[] = [];
let sequence = 0;
const unique = (prefix: string) => `${prefix}.${String(Date.now())}.${String(++sequence)}`;

beforeAll(async () => {
  container = await new RabbitMQContainer('rabbitmq:4-management').start();
  url = container.getAmqpUrl();
  observer = await amqp.connect(url);
  channel = await observer.createChannel();
}, 120_000);

afterEach(async () => {
  await Promise.all(transports.splice(0).map((transport) => transport.disconnect()));
});

afterAll(async () => {
  await observer.close();
  await container.stop();
});

async function connectTransport(options: { controlQueue?: string } = {}) {
  const controlQueue = options.controlQueue ?? unique('control');
  const transport = new RabbitMQTransport({ url, controlQueue, recovery: { initialDelay: 100, maxDelay: 500 } });
  transports.push(transport);
  await transport.connect();
  return { transport, controlQueue };
}

async function waitFor<T>(probe: () => Promise<T | undefined | false>, timeoutMs = 10_000): Promise<T> {
  const deadline = Date.now() + timeoutMs;
  for (;;) {
    const value = await probe();
    if (value !== undefined && value !== false) return value;
    if (Date.now() > deadline) throw new Error('Timed out waiting');
    await new Promise((resolve) => setTimeout(resolve, 50));
  }
}

const receive = (queue: string, timeoutMs?: number) =>
  waitFor<GetMessage>(() => channel.get(queue, { noAck: true }), timeoutMs);

function operation(overrides: Partial<TransportOperation> = {}): TransportOperation {
  return {
    messageId: unique('message'),
    intent: 'send',
    messageType: 'Sales.PlaceOrder',
    headers: { 'zusammen.message-type': 'Sales.PlaceOrder', tenant: 'acme' },
    body: new TextEncoder().encode('{"orderId":"o1"}'),
    properties: { contentType: 'application/json' },
    ...overrides,
  };
}

const control = (sessionId = unique('session')): ControlMessage => ({
  sessionId,
  remainingCommitDurationMs: 15_000,
  commitDelayIncrementMs: 1_000,
  attempt: 1,
  failures: 0,
});

describe('rabbitmq transport: dispatch', () => {
  test('sends go to the destination queue with native properties', async () => {
    const { transport } = await connectTransport();
    const queue = unique('billing');
    await channel.assertQueue(queue, { autoDelete: true });
    const sent = operation({
      destination: queue,
      properties: { contentType: 'application/json', correlationId: 'c1', replyTo: 'sales' },
    });

    await transport.dispatch([sent]);

    const message = await receive(queue);
    expect(message.content).toEqual(Buffer.from(sent.body));
    expect(message.properties).toMatchObject({
      messageId: sent.messageId,
      type: 'Sales.PlaceOrder',
      contentType: 'application/json',
      correlationId: 'c1',
      replyTo: 'sales',
      deliveryMode: 2,
      headers: { 'zusammen.message-type': 'Sales.PlaceOrder', tenant: 'acme' },
    });
  });

  test('publishes go to the events exchange, routed by topic or message type', async () => {
    const { transport } = await connectTransport();
    const queue = unique('subscriber');
    await channel.assertQueue(queue, { autoDelete: true });
    await channel.bindQueue(queue, ZUSAMMEN_EVENTS_EXCHANGE, 'sales.orders');
    await channel.bindQueue(queue, ZUSAMMEN_EVENTS_EXCHANGE, 'Sales.OrderPlaced');

    const byTopic = operation({ intent: 'publish', topic: 'sales.orders' });
    const byType = operation({ intent: 'publish', messageType: 'Sales.OrderPlaced' });
    await transport.dispatch([byTopic, byType]);

    const ids = [(await receive(queue)).properties.messageId, (await receive(queue)).properties.messageId];
    expect(ids.sort()).toEqual([byTopic.messageId, byType.messageId].sort());
  });

  test('publishing without subscribers succeeds', async () => {
    const { transport } = await connectTransport();
    await expect(
      transport.dispatch([operation({ intent: 'publish', topic: unique('nobody') })]),
    ).resolves.toBeUndefined();
  });

  test('an unroutable send fails, and later dispatches still work', async () => {
    const { transport } = await connectTransport();
    const missing = operation({ destination: unique('missing') });

    await expect(transport.dispatch([missing])).rejects.toBeInstanceOf(UnroutableMessageError);

    const queue = unique('billing');
    await channel.assertQueue(queue, { autoDelete: true });
    await transport.dispatch([operation({ destination: queue })]);
    await receive(queue);
  });
});

describe('rabbitmq transport: control messages', () => {
  async function consume(results: (message: ControlMessage) => ControlResult) {
    const { transport, controlQueue } = await connectTransport();
    const received: { message: ControlMessage; at: number }[] = [];
    const stop = await transport.consumeControl((message) => {
      received.push({ message, at: Date.now() });
      return Promise.resolve(results(message));
    });
    return { transport, controlQueue, received, stop };
  }

  test('control messages are delivered to the handler and acknowledged', async () => {
    const { transport, controlQueue, received } = await consume(() => ({ kind: 'ack' }));
    const message = control();

    await transport.sendControl(message, 0);

    await waitFor(() => Promise.resolve(received.length === 1));
    expect(received[0]?.message).toEqual(message);
    await new Promise((resolve) => setTimeout(resolve, 200));
    expect((await channel.checkQueue(controlQueue)).messageCount).toBe(0);
  });

  test('retries come back after the delay, as the next control message', async () => {
    const { transport, received } = await consume((message) =>
      message.attempt === 1
        ? { kind: 'retry', delayMs: 800, next: { ...message, attempt: 2, remainingCommitDurationMs: 14_200 } }
        : { kind: 'ack' },
    );

    await transport.sendControl(control(), 0);

    await waitFor(() => Promise.resolve(received.length === 2), 15_000);
    const [first, second] = received;
    expect(second?.message).toMatchObject({ attempt: 2, remainingCommitDurationMs: 14_200 });
    expect((second?.at ?? 0) - (first?.at ?? 0)).toBeGreaterThanOrEqual(900); // rounded up to the 1 s level
  });

  test('errors move the control message to the error queue', async () => {
    const { transport, controlQueue } = await consume(() => ({
      kind: 'error',
      error: new Error('dispatch keeps failing'),
    }));
    const message = control();

    await transport.sendControl(message, 0);

    const failed = await receive(`${controlQueue}.error`);
    expect(JSON.parse(failed.content.toString())).toEqual(message);
    expect(failed.properties.headers).toMatchObject({ [ERROR_HEADER]: 'dispatch keeps failing' });
  });

  test('malformed control messages move to the error queue', async () => {
    const { controlQueue, received } = await consume(() => ({ kind: 'ack' }));

    channel.sendToQueue(controlQueue, Buffer.from('not json'));

    const failed = await receive(`${controlQueue}.error`);
    expect(failed.content.toString()).toBe('not json');
    expect(received).toHaveLength(0);
  });

  test('delayed control messages arrive after the delay', async () => {
    const { transport, received } = await consume(() => ({ kind: 'ack' }));
    const sentAt = Date.now();

    await transport.sendControl(control(), 500);

    await waitFor(() => Promise.resolve(received.length === 1));
    expect((received[0]?.at ?? 0) - sentAt).toBeGreaterThanOrEqual(900); // rounded up to the 1 s level
  });

  test('stopping leaves control messages in the queue', async () => {
    const { transport, controlQueue, received, stop } = await consume(() => ({ kind: 'ack' }));
    await stop();

    await transport.sendControl(control(), 0);

    await new Promise((resolve) => setTimeout(resolve, 300));
    expect(received).toHaveLength(0);
    expect((await channel.checkQueue(controlQueue)).messageCount).toBe(1);
  });

  test('sending control messages to a missing queue fails', async () => {
    const { transport, controlQueue } = await connectTransport();
    await channel.deleteQueue(controlQueue);

    await expect(transport.sendControl(control(), 0)).rejects.toBeInstanceOf(UnroutableMessageError);
  });

  test('reconnects and resumes consuming after the broker drops the connection', async () => {
    const { transport, received } = await consume(() => ({ kind: 'ack' }));

    const result = await container.exec(['rabbitmqctl', 'close_all_connections', 'test']);
    expect(result.exitCode).toBe(0);
    observer = await amqp.connect(url);
    channel = await observer.createChannel();

    await waitFor(async () => {
      try {
        await transport.sendControl(control(), 0);
        return true;
      } catch {
        return false;
      }
    });
    await waitFor(() => Promise.resolve(received.length >= 1));
  });
});

describe('rabbitmq transport with transactional sessions', () => {
  class OrderPlaced {
    constructor(readonly orderId: string) {}
  }

  test('committed messages are delivered, also when the immediate dispatch fails', async () => {
    const { transport } = await connectTransport();
    const queue = unique('subscriber');
    await channel.assertQueue(queue, { autoDelete: true });
    await channel.bindQueue(queue, ZUSAMMEN_EVENTS_EXCHANGE, 'OrderPlaced');

    let failNextDispatch = false;
    const flakyTransport: TransportProvider = {
      connect: () => transport.connect(),
      disconnect: () => transport.disconnect(),
      sendControl: (message) => transport.sendControl(message, 0),
      consumeControl: (handler) => transport.consumeControl(handler),
      dispatch: (operations) => {
        if (failNextDispatch) {
          failNextDispatch = false;
          return Promise.reject(new Error('simulated crash before dispatch'));
        }
        return transport.dispatch(operations);
      },
    };
    const factory = createSessionFactory({
      persistence: new InMemoryPersistence(),
      transport: flakyTransport,
      controlTiming: { initialCommitDelayIncrementMs: 500 },
    });
    await factory.start();

    const happy = await factory.open();
    await happy.publish(new OrderPlaced('o1'));
    await happy.commit();

    failNextDispatch = true;
    const crashed = await factory.open();
    await crashed.publish(new OrderPlaced('o2'));
    await crashed.commit();

    // At-least-once: duplicates are allowed, missing messages are not
    const bodies = new Set<string>();
    await waitFor(async () => {
      const message = await channel.get(queue, { noAck: true });
      if (message !== false) bodies.add(message.content.toString());
      return bodies.size === 2;
    }, 15_000);
    expect([...bodies].sort()).toEqual(['{"orderId":"o1"}', '{"orderId":"o2"}']);
    await factory.stop();
  });
});
