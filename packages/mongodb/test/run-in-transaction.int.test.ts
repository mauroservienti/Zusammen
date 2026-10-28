import { MongoDBContainer, type StartedMongoDBContainer } from '@testcontainers/mongodb';
import { MongoServerError, type ClientSession } from 'mongodb';
import mongoose, { type Connection, type Model } from 'mongoose';
import { afterAll, beforeAll, describe, expect, test } from 'vitest';
import { createSessionFactory, SessionCommitConflictError } from '@zusammen/core';
import { MongoDBPersistence } from '@zusammen/mongodb';
import { InMemoryTransport, MONGODB_IMAGE } from '@zusammen/testing';

class OrderPlaced {
  constructor(readonly orderId: string) {}
}

interface Order {
  _id: string;
  total: number;
}

let container: StartedMongoDBContainer;
let connection: Connection;
let database = 0;

beforeAll(async () => {
  container = await new MongoDBContainer(MONGODB_IMAGE).start();
  connection = await mongoose.createConnection(container.getConnectionString(), { directConnection: true }).asPromise();
}, 120_000);

afterAll(async () => {
  await connection.close();
  await container.stop();
});

/** A factory on a fresh database, sharing Mongoose's client, so tests don't see each other's data. */
async function setup() {
  const db = connection.useDb(`zusammen_tx_${String(++database)}`);
  const orders: Model<Order> = db.model<Order>('Order', new mongoose.Schema<Order>({ _id: String, total: Number }));
  await orders.createCollection();
  const persistence = new MongoDBPersistence({ client: connection.getClient(), databaseName: db.name });
  const transport = new InMemoryTransport();
  const factory = createSessionFactory({ persistence, transport, createResources: true, maxCommitDurationMs: 15_000 });
  await factory.start();
  // Mongoose's transaction API: commits when the callback returns, retries it on transient errors
  const runTransaction = <T>(work: (session: ClientSession) => Promise<T>) =>
    db.transaction((session) => work(session));
  return { db, orders, persistence, transport, factory, runTransaction };
}

describe('mongodb: sessions in a transaction the application runs', () => {
  test('Mongoose: commits the data and the outbox record together, then dispatches', async () => {
    const { orders, persistence, transport, factory, runTransaction } = await setup();

    const sessionId = await factory.runInTransaction(runTransaction, async (session) => {
      await orders.create([{ _id: 'order-1', total: 42 }], { session: session.transactionContext });
      await session.publish(new OrderPlaced('order-1'));
      return session.sessionId;
    });

    expect(await orders.findById('order-1').lean()).toEqual({ _id: 'order-1', total: 42, __v: 0 });
    expect(transport.dispatched).toHaveLength(1);
    expect(await persistence.get(sessionId)).toMatchObject({ dispatched: true });

    await transport.drain();
    expect(transport.dispatched).toHaveLength(1);
  });

  test('Mongoose: the work throws, nothing is stored or sent', async () => {
    const { orders, persistence, transport, factory, runTransaction } = await setup();

    let sessionId = '';
    const run = factory.runInTransaction(runTransaction, async (session) => {
      sessionId = session.sessionId;
      await orders.create([{ _id: 'order-1', total: 42 }], { session: session.transactionContext });
      await session.publish(new OrderPlaced('order-1'));
      throw new Error('out of stock');
    });

    await expect(run).rejects.toThrow('out of stock');
    await transport.drain();
    expect(await orders.findById('order-1')).toBeNull();
    expect(await persistence.get(sessionId)).toBeNull();
    expect(transport.dispatched).toHaveLength(0);
  });

  test('Mongoose: a tombstone stored first fails the outbox insert, and the data rolls back', async () => {
    const { orders, persistence, transport, factory, runTransaction } = await setup();

    const run = factory.runInTransaction(runTransaction, async (session) => {
      await orders.create([{ _id: 'order-1', total: 42 }], { session: session.transactionContext });
      await session.publish(new OrderPlaced('order-1'));
      await persistence.storeTombstone(session.sessionId);
    });

    await expect(run).rejects.toBeInstanceOf(SessionCommitConflictError);
    await transport.drain();
    expect(await orders.findById('order-1')).toBeNull();
    expect(transport.dispatched).toHaveLength(0);
  });

  test('Mongoose: a retried callback gets a new session per attempt', async () => {
    const { orders, transport, factory, runTransaction } = await setup();

    const sessionIds: string[] = [];
    await factory.runInTransaction(runTransaction, async (session) => {
      sessionIds.push(session.sessionId);
      await orders.create([{ _id: 'order-1', total: 42 }], { session: session.transactionContext });
      await session.publish(new OrderPlaced('order-1'));
      if (sessionIds.length === 1) {
        throw new MongoServerError({
          message: 'write conflict',
          code: 112,
          errorLabels: ['TransientTransactionError'],
        });
      }
    });
    await transport.drain();

    expect(new Set(sessionIds).size).toBe(2);
    expect(await orders.countDocuments()).toBe(1);
    expect(transport.dispatched).toHaveLength(1);
  });

  test("the driver's withTransaction works the same way", async () => {
    const { db, orders, persistence, transport, factory } = await setup();
    const client = connection.getClient();

    const sessionId = await factory.runInTransaction(
      (work) => client.withSession((session) => session.withTransaction(() => work(session))),
      async (session) => {
        await client
          .db(db.name)
          .collection<Order>('orders')
          .insertOne({ _id: 'order-1', total: 42 }, { session: session.transactionContext });
        await session.publish(new OrderPlaced('order-1'));
        return session.sessionId;
      },
    );

    expect(await orders.countDocuments()).toBe(1);
    expect(transport.dispatched).toHaveLength(1);
    expect(await persistence.get(sessionId)).toMatchObject({ dispatched: true });
  });
});
