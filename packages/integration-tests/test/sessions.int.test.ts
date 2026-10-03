import { afterAll, afterEach, beforeAll, describe, expect, test } from 'vitest';
import type { Db } from 'mongodb';
import { SessionCommitConflictError } from '@zusammen/core';
import { Environment, sleep, unique, waitFor } from './environment.js';

class OrderPlaced {
  constructor(readonly orderId: string) {}
}

const environment = new Environment();
let database: Db;
let controlQueue: string;
let topic: string;

beforeAll(() => environment.start(), 180_000);
afterAll(() => environment.stop());
afterEach(() => environment.stopFactories());

async function setupTest() {
  database = environment.client.db(unique('zusammen').replaceAll('.', '_'));
  // Concurrent transactions can't implicitly create the same collection
  await database.createCollection('orders');
  controlQueue = unique('control');
  topic = unique('orders');
}

const orders = () => database.collection<{ _id: string }>('orders');

async function placeOrder(instance: Awaited<ReturnType<Environment['instance']>>, orderId: string) {
  const session = await instance.factory.open();
  await orders().insertOne({ _id: orderId }, { session: session.transactionContext });
  await session.publish(new OrderPlaced(orderId), { topic });
  await session.commit();
  return session.sessionId;
}

describe('transactional sessions on MongoDB and RabbitMQ', () => {
  test('happy path: order stored, event delivered, outbox record dispatched', async () => {
    await setupTest();
    const instance = await environment.instance({ database, controlQueue });
    const subscriber = await environment.subscribe(topic);

    const sessionId = await placeOrder(instance, 'o1');

    await waitFor(() => subscriber.received.length === 1);
    expect(subscriber.bodies()).toEqual(new Set(['{"orderId":"o1"}']));
    expect(await orders().findOne({ _id: 'o1' })).not.toBeNull();
    expect(await instance.outbox.findOne({ _id: sessionId })).toMatchObject({ Dispatched: true });

    // The delayed control message finds everything done and doesn't dispatch again
    await sleep(2_000);
    expect(subscriber.received).toHaveLength(1);
  });

  test('rollback: nothing stored, nothing sent', async () => {
    await setupTest();
    const instance = await environment.instance({ database, controlQueue });
    const subscriber = await environment.subscribe(topic);

    const session = await instance.factory.open();
    await orders().insertOne({ _id: 'o1' }, { session: session.transactionContext });
    await session.publish(new OrderPlaced('o1'), { topic });
    await session.rollback();

    await sleep(1_500);
    expect(subscriber.received).toHaveLength(0);
    expect(await orders().countDocuments()).toBe(0);
    expect(await instance.outbox.countDocuments()).toBe(0);
    expect((await environment.channel.checkQueue(controlQueue)).messageCount).toBe(0);
  });

  test('crash before commit: the control message stores a tombstone once the window expires', async () => {
    await setupTest();
    const instance = await environment.instance({ database, controlQueue });
    const subscriber = await environment.subscribe(topic);
    instance.faults.failCommit = true;

    const session = await instance.factory.open();
    await orders().insertOne({ _id: 'o1' }, { session: session.transactionContext });
    await session.publish(new OrderPlaced('o1'), { topic });
    await expect(session.commit()).rejects.toThrow('injected crash before commit');

    await waitFor(async () => (await instance.outbox.findOne({ _id: session.sessionId })) !== null);
    expect(await instance.outbox.findOne({ _id: session.sessionId })).toMatchObject({
      Dispatched: true,
      TransportOperations: [],
    });
    expect(subscriber.received).toHaveLength(0);
    expect(await orders().countDocuments()).toBe(0);
  });

  test('crash after commit, before dispatch: the control message delivers', async () => {
    await setupTest();
    const instance = await environment.instance({ database, controlQueue });
    const subscriber = await environment.subscribe(topic);
    instance.faults.failingDispatches = 1;

    const sessionId = await placeOrder(instance, 'o1');
    expect(await instance.outbox.findOne({ _id: sessionId })).toMatchObject({ Dispatched: false });

    await waitFor(() => subscriber.received.length === 1);
    await waitFor(async () => (await instance.outbox.findOne({ _id: sessionId }))?.Dispatched === true);
  });

  test('control message before the commit: retries until the commit lands, then delivers', async () => {
    await setupTest();
    const instance = await environment.instance({ database, controlQueue });
    const subscriber = await environment.subscribe(topic);
    instance.faults.beforeCommit = async () => {
      await sleep(2_000); // longer than the initial control message delay
    };
    instance.faults.failingDispatches = 1;

    await placeOrder(instance, 'o1');

    await waitFor(() => subscriber.received.length === 1);
    expect(await orders().findOne({ _id: 'o1' })).not.toBeNull();
  });

  test('outbox write slower than the window: the tombstone wins and the commit fails', async () => {
    await setupTest();
    const instance = await environment.instance({ database, controlQueue, maxCommitDurationMs: 1_000 });
    const subscriber = await environment.subscribe(topic);
    let sessionId = '';
    instance.faults.beforeStoreOutbox = () =>
      waitFor(async () => (await instance.outbox.findOne({ _id: sessionId })) !== null);

    const session = await instance.factory.open();
    sessionId = session.sessionId;
    await orders().insertOne({ _id: 'o1' }, { session: session.transactionContext });
    await session.publish(new OrderPlaced('o1'), { topic });

    await expect(session.commit()).rejects.toBeInstanceOf(SessionCommitConflictError);
    expect(await orders().countDocuments()).toBe(0);
    await sleep(1_000);
    expect(subscriber.received).toHaveLength(0);
  });

  test('the committing process dies: another instance delivers', async () => {
    await setupTest();
    const dying = await environment.instance({ database, controlQueue });
    const survivor = await environment.instance({ database, controlQueue });
    const subscriber = await environment.subscribe(topic);
    dying.faults.failingDispatches = 1;

    await placeOrder(dying, 'o1');
    await dying.factory.stop(); // before the delayed control message is due

    await waitFor(() => subscriber.received.length === 1);
    expect(survivor).toBeDefined();
  });

  test('concurrent sessions on competing instances: every committed order is delivered', async () => {
    await setupTest();
    const first = await environment.instance({ database, controlQueue });
    const second = await environment.instance({ database, controlQueue });
    first.faults.failEveryNthDispatch = 4;
    second.faults.failEveryNthDispatch = 4;
    const subscriber = await environment.subscribe(topic);

    const orderIds = Array.from({ length: 40 }, (_, i) => `o${String(i)}`);
    await Promise.all(orderIds.map((orderId, i) => placeOrder(i % 2 === 0 ? first : second, orderId)));

    await waitFor(() => subscriber.bodies().size === orderIds.length, 30_000);
    expect(await orders().countDocuments()).toBe(orderIds.length);
    await waitFor(async () => (await first.outbox.countDocuments({ Dispatched: false })) === 0, 30_000);
  });
});
