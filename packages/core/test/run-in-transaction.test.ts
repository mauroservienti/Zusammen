import { describe, expect, test } from 'vitest';
import {
  ApplicationTransactionError,
  createSessionFactory,
  FactoryNotStartedError,
  getSession,
  SessionCommitConflictError,
  ZusammenError,
  type CreateSessionFactoryOptions,
  type PersistenceProvider,
  type TransactionalSession,
} from '@zusammen/core';
import { InMemoryPersistence, InMemoryTransport, type InMemoryTransaction } from '@zusammen/testing';

class OrderPlaced {
  constructor(readonly orderId: string) {}
}

async function setup(options: Partial<CreateSessionFactoryOptions<InMemoryTransaction>> = {}) {
  const persistence = new InMemoryPersistence();
  const transport = new InMemoryTransport();
  let id = 0;
  const factory = createSessionFactory({
    persistence,
    transport,
    maxCommitDurationMs: 15_000,
    newId: () => `id-${String(++id)}`,
    ...options,
  });
  await factory.start();
  // The application's transaction API, e.g. `(work) => prisma.$transaction(work)`
  const runTransaction = <T>(work: (tx: InMemoryTransaction) => Promise<T>) => persistence.transaction(work);
  return { persistence, transport, factory, runTransaction };
}

describe('sessions in a transaction the application runs', () => {
  test('happy path: commits data and messages with the application transaction, then dispatches', async () => {
    const { persistence, transport, factory, runTransaction } = await setup();
    let current: TransactionalSession<InMemoryTransaction> | undefined;

    const result = await factory.runInTransaction(runTransaction, async (session) => {
      current = session;
      session.transactionContext.writes.set('order-1', { total: 42 });
      await getSession().publish(new OrderPlaced('order-1'));
      return 'placed';
    });

    expect(result).toBe('placed');
    expect(current?.status).toBe('committed');
    expect(persistence.data.get('order-1')).toEqual({ total: 42 });
    expect(transport.dispatched).toHaveLength(1);
    expect(persistence.records.get(current?.sessionId ?? '')?.dispatched).toBe(true);

    await transport.drain();
    expect(transport.dispatched).toHaveLength(1);
    expect(transport.processed.map((p) => p.result.kind)).toEqual(['ack']);
  });

  test('the work throws: the transaction rolls back, nothing is stored or sent', async () => {
    const { persistence, transport, factory, runTransaction } = await setup();
    let current: TransactionalSession<InMemoryTransaction> | undefined;

    const run = factory.runInTransaction(runTransaction, async (session) => {
      current = session;
      session.transactionContext.writes.set('order-1', {});
      await session.publish(new OrderPlaced('order-1'));
      throw new Error('out of stock');
    });

    await expect(run).rejects.toThrow('out of stock');
    expect(current?.status).toBe('rolledBack');
    expect(persistence.data.size).toBe(0);
    expect(transport.dispatched).toHaveLength(0);
    expect(transport.controlQueue).toHaveLength(0);
  });

  test('the application commit fails after the outbox write: the control message stores a tombstone', async () => {
    const { persistence, transport, factory } = await setup();
    const failingCommit = async <T>(work: (tx: InMemoryTransaction) => Promise<T>) => {
      await work(await persistence.begin());
      throw new Error('connection lost during commit');
    };

    let sessionId = '';
    const run = factory.runInTransaction(failingCommit, async (session) => {
      sessionId = session.sessionId;
      await session.publish(new OrderPlaced('order-1'));
    });

    await expect(run).rejects.toThrow('connection lost during commit');
    await transport.drain();
    expect(transport.dispatched).toHaveLength(0);
    expect(persistence.records.get(sessionId)).toMatchObject({ dispatched: true, transportOperations: [] });
  });

  test('commit slower than the window: the tombstone wins and the application transaction fails', async () => {
    const { persistence, transport, factory, runTransaction } = await setup();
    persistence.beforeCommit = () => transport.drain();

    const run = factory.runInTransaction(
      runTransaction,
      async (session) => {
        session.transactionContext.writes.set('order-1', {});
        await session.publish(new OrderPlaced('order-1'));
      },
      { maxCommitDurationMs: 3_000 },
    );

    await expect(run).rejects.toBeInstanceOf(SessionCommitConflictError);
    expect(persistence.data.size).toBe(0);
    expect(transport.dispatched).toHaveLength(0);
  });

  test('a tombstone stored before the outbox write fails the work, so the application rolls back', async () => {
    const { persistence, factory, runTransaction } = await setup();

    const run = factory.runInTransaction(runTransaction, async (session) => {
      session.transactionContext.writes.set('order-1', {});
      await session.publish(new OrderPlaced('order-1'));
      await persistence.storeTombstone(session.sessionId);
    });

    await expect(run).rejects.toBeInstanceOf(SessionCommitConflictError);
    expect(persistence.data.size).toBe(0);
  });

  test('immediate dispatch fails: the control message dispatches', async () => {
    const { persistence, transport, factory, runTransaction } = await setup();
    transport.failingDispatches = 1;

    let sessionId = '';
    await factory.runInTransaction(runTransaction, async (session) => {
      sessionId = session.sessionId;
      await session.publish(new OrderPlaced('order-1'));
    });

    expect(transport.dispatched).toHaveLength(0);
    await transport.drain();
    expect(transport.dispatched).toHaveLength(1);
    expect(persistence.records.get(sessionId)?.dispatched).toBe(true);
  });

  test('a transaction API that retries its callback gets a new session per attempt', async () => {
    const { persistence, transport, factory } = await setup();
    let attempts = 0;
    // Like a retry on a serialization failure: the first attempt's commit fails, the callback runs again
    const retrying = async <T>(work: (tx: InMemoryTransaction) => Promise<T>): Promise<T> => {
      for (;;) {
        const tx = await persistence.begin();
        const result = await work(tx);
        if (++attempts > 1) {
          await persistence.commit(tx);
          return result;
        }
        await persistence.rollback(tx);
      }
    };

    const sessionIds: string[] = [];
    await factory.runInTransaction(retrying, async (session) => {
      sessionIds.push(session.sessionId);
      await session.publish(new OrderPlaced(`attempt-${String(sessionIds.length)}`));
    });
    await transport.drain();

    expect(sessionIds).toHaveLength(2);
    expect(new Set(sessionIds).size).toBe(2);
    expect(transport.dispatched.map((operation) => new TextDecoder().decode(operation.body))).toEqual([
      '{"orderId":"attempt-2"}',
    ]);
    expect(persistence.records.get(sessionIds[0] ?? '')).toMatchObject({ transportOperations: [] });
  });

  test('an empty session commits data without a control message or outbox record', async () => {
    const { persistence, transport, factory, runTransaction } = await setup();

    await factory.runInTransaction(runTransaction, (session) => {
      session.transactionContext.writes.set('order-1', {});
      return Promise.resolve();
    });

    expect(persistence.data.has('order-1')).toBe(true);
    expect(transport.controlQueue).toHaveLength(0);
    expect(persistence.records.size).toBe(0);
  });

  test("the session's commit and rollback aren't available: the application owns the transaction", async () => {
    const { factory, runTransaction } = await setup();

    await factory.runInTransaction(runTransaction, async (session) => {
      await expect(session.commit()).rejects.toBeInstanceOf(ApplicationTransactionError);
      await expect(session.rollback()).rejects.toBeInstanceOf(ApplicationTransactionError);
      await session[Symbol.asyncDispose]();
      expect(session.status).toBe('open');
    });
  });

  test('before start: throws', async () => {
    const factory = createSessionFactory({
      persistence: new InMemoryPersistence(),
      transport: new InMemoryTransport(),
    });
    const persistence = new InMemoryPersistence();

    await expect(
      factory.runInTransaction(
        (work) => persistence.transaction(work),
        () => Promise.resolve(),
      ),
    ).rejects.toBeInstanceOf(FactoryNotStartedError);
  });

  test('providers that only store outbox records support runInTransaction, but not open', async () => {
    const memory = new InMemoryPersistence();
    const outboxOnly: PersistenceProvider<InMemoryTransaction> = {
      connect: () => memory.connect(),
      disconnect: () => memory.disconnect(),
      storeOutbox: (record, tx) => memory.storeOutbox(record, tx),
      get: (sessionId) => memory.get(sessionId),
      markDispatched: (sessionId) => memory.markDispatched(sessionId),
      storeTombstone: (sessionId) => memory.storeTombstone(sessionId),
    };
    const transport = new InMemoryTransport();
    const factory = createSessionFactory({ persistence: outboxOnly, transport });
    await factory.start();

    await expect(factory.open()).rejects.toBeInstanceOf(ZusammenError);
    await factory.runInTransaction(
      (work) => memory.transaction(work),
      (session) => session.publish(new OrderPlaced('order-1')),
    );
    expect(transport.dispatched).toHaveLength(1);
  });
});
