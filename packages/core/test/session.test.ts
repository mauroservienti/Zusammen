import { describe, expect, test } from 'vitest';
import {
  createSessionFactory,
  FactoryNotStartedError,
  IncompatibleConventionError,
  SessionClosedError,
  SessionCommitConflictError,
  type CreateSessionFactoryOptions,
  type MessageConvention,
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
  return { persistence, transport, factory };
}

describe('transactional session', () => {
  test('happy path: commits data and messages, dispatches once, control message is a no-op', async () => {
    const { persistence, transport, factory } = await setup();

    const session = await factory.open();
    session.transactionContext.writes.set('order-1', { total: 42 });
    await session.publish(new OrderPlaced('order-1'));
    await session.send('billing', new OrderPlaced('order-1'));
    await session.commit();

    expect(persistence.data.get('order-1')).toEqual({ total: 42 });
    expect(transport.dispatched).toHaveLength(2);
    expect(persistence.records.get(session.sessionId)?.dispatched).toBe(true);

    await transport.drain();
    expect(transport.dispatched).toHaveLength(2);
    expect(transport.processed.map((p) => p.result.kind)).toEqual(['ack']);
  });

  test('the control message is delayed so it normally finds the immediate dispatch done', async () => {
    const { transport, factory } = await setup({ controlTiming: { initialCommitDelayIncrementMs: 750 } });

    const session = await factory.open({ maxCommitDurationMs: 5_000 });
    await session.publish(new OrderPlaced('order-1'));
    await session.commit();

    expect(transport.controlDelays).toEqual([750]);
    expect(transport.controlQueue[0]?.remainingCommitDurationMs).toBe(5_000);
  });

  test('rollback: nothing persisted, nothing dispatched, no control message', async () => {
    const { persistence, transport, factory } = await setup();

    const session = await factory.open();
    session.transactionContext.writes.set('order-1', {});
    await session.publish(new OrderPlaced('order-1'));
    await session.rollback();

    expect(persistence.data.size).toBe(0);
    expect(transport.dispatched).toHaveLength(0);
    expect(transport.controlQueue).toHaveLength(0);
  });

  test('empty session: commits data without control message or outbox record', async () => {
    const { persistence, transport, factory } = await setup();

    const session = await factory.open();
    session.transactionContext.writes.set('order-1', {});
    await session.commit();

    expect(persistence.data.has('order-1')).toBe(true);
    expect(transport.controlQueue).toHaveLength(0);
    expect(persistence.records.size).toBe(0);
  });

  test('crash or failure after commit: the control message dispatches', async () => {
    const { persistence, transport, factory } = await setup();
    transport.failingDispatches = 1;

    const session = await factory.open();
    await session.publish(new OrderPlaced('order-1'));
    await session.commit();

    expect(transport.dispatched).toHaveLength(0);
    expect(persistence.records.get(session.sessionId)?.dispatched).toBe(false);

    await transport.drain();
    expect(transport.dispatched).toHaveLength(1);
    expect(persistence.records.get(session.sessionId)?.dispatched).toBe(true);
  });

  test('control message before commit: retries until the record appears, then dispatches', async () => {
    const { persistence, transport, factory } = await setup();
    transport.failingDispatches = 1; // immediate dispatch fails, so the control message must deliver
    persistence.beforeCommit = async () => {
      expect((await transport.processNext()).result.kind).toBe('retry');
      expect((await transport.processNext()).result.kind).toBe('retry');
    };

    const session = await factory.open();
    await session.publish(new OrderPlaced('order-1'));
    await session.commit();
    await transport.drain();

    expect(transport.dispatched).toHaveLength(1);
    expect(transport.processed.map((p) => p.result.kind)).toEqual(['retry', 'retry', 'ack']);
  });

  test('commit slower than the window: tombstone wins, commit throws, nothing persisted or dispatched', async () => {
    const { persistence, transport, factory } = await setup();
    persistence.beforeCommit = () => transport.drain();

    const session = await factory.open({ maxCommitDurationMs: 3_000 });
    session.transactionContext.writes.set('order-1', {});
    await session.publish(new OrderPlaced('order-1'));

    await expect(session.commit()).rejects.toBeInstanceOf(SessionCommitConflictError);
    expect(persistence.data.size).toBe(0);
    expect(transport.dispatched).toHaveLength(0);
    expect(persistence.records.get(session.sessionId)).toMatchObject({ dispatched: true, transportOperations: [] });
    expect(transport.processed.map((p) => p.result.kind)).toEqual(['retry', 'retry', 'ack']);
  });

  test('control message cannot be sent: the commit fails and rolls back', async () => {
    const { persistence, transport, factory } = await setup();
    transport.failSendControl = true;

    const session = await factory.open();
    session.transactionContext.writes.set('order-1', {});
    await session.publish(new OrderPlaced('order-1'));

    await expect(session.commit()).rejects.toThrow('broker unavailable');
    expect(persistence.data.size).toBe(0);
    expect(persistence.records.size).toBe(0);
    await expect(session.commit()).rejects.toBeInstanceOf(SessionClosedError);
  });

  test('sessions are single use', async () => {
    const { factory } = await setup();

    const session = await factory.open();
    await session.commit();

    await expect(session.publish(new OrderPlaced('x'))).rejects.toThrow('is already committed');
    await expect(session.commit()).rejects.toBeInstanceOf(SessionClosedError);
    await expect(session.rollback()).rejects.toBeInstanceOf(SessionClosedError);
  });

  test('disposing an uncommitted session rolls it back', async () => {
    const { persistence, transport, factory } = await setup();

    {
      await using session = await factory.open();
      session.transactionContext.writes.set('order-1', {});
      await session.publish(new OrderPlaced('order-1'));
    }

    expect(persistence.data.size).toBe(0);
    expect(transport.controlQueue).toHaveLength(0);
  });

  test('disposing a committed session is a no-op', async () => {
    const { persistence, factory } = await setup();

    {
      await using session = await factory.open();
      session.transactionContext.writes.set('order-1', {});
      await session.commit();
    }

    expect(persistence.data.size).toBe(1);
  });

  test('re-dispatch puts identical messages on the wire', async () => {
    const { persistence, transport, factory } = await setup();

    const session = await factory.open();
    await session.publish(new OrderPlaced('order-1'));
    await session.commit();
    const first = transport.dispatched[0];

    const record = persistence.records.get(session.sessionId);
    if (record !== undefined) record.dispatched = false; // e.g. crash between dispatch and markDispatched
    await transport.drain();

    expect(transport.dispatched).toHaveLength(2);
    expect(transport.dispatched[1]).toEqual(first);
  });

  test('unresolvable message type fails at publish time, before commit', async () => {
    const { factory } = await setup();

    const session = await factory.open();
    await expect(session.publish({ orderId: 'x' })).rejects.toThrow('cannot determine the message type');
  });
});

describe('session factory', () => {
  test('open before start throws', async () => {
    const factory = createSessionFactory({
      persistence: new InMemoryPersistence(),
      transport: new InMemoryTransport(),
    });
    await expect(factory.open()).rejects.toBeInstanceOf(FactoryNotStartedError);
  });

  test('start connects providers and starts control processing; stop reverses it', async () => {
    const { persistence, transport, factory } = await setup();
    expect(persistence.connected).toBe(true);
    expect(transport.connected).toBe(true);
    expect(transport.handler).toBeDefined();

    await factory.stop();
    expect(transport.handler).toBeUndefined();
    expect(transport.connected).toBe(false);
    expect(persistence.connected).toBe(false);
    await expect(factory.open()).rejects.toBeInstanceOf(FactoryNotStartedError);
  });

  test('the transport can reject the convention at startup', async () => {
    const transport = new InMemoryTransport();
    transport.validateConvention = (convention: MessageConvention) => {
      throw new IncompatibleConventionError(convention.name, 'requires the nservicebus convention');
    };
    const factory = createSessionFactory({ persistence: new InMemoryPersistence(), transport });

    await expect(factory.start()).rejects.toBeInstanceOf(IncompatibleConventionError);
    expect(transport.connected).toBe(false);
  });

  test('rejects invalid commit durations', () => {
    expect(() =>
      createSessionFactory({
        persistence: new InMemoryPersistence(),
        transport: new InMemoryTransport(),
        maxCommitDurationMs: Number.NaN,
      }),
    ).toThrow('maxCommitDurationMs');
  });

  test('independent factories: one commits, the other rolls back', async () => {
    const alpha = await setup();
    const beta = await setup();

    const a = await alpha.factory.open();
    await a.publish(new OrderPlaced('a'));
    const b = await beta.factory.open();
    await b.publish(new OrderPlaced('b'));

    await a.commit();
    await b.rollback();
    await alpha.transport.drain();
    await beta.transport.drain();

    expect(alpha.transport.dispatched).toHaveLength(1);
    expect(beta.transport.dispatched).toHaveLength(0);
  });

  test('concurrent sessions dispatch every committed message', async () => {
    const { transport, factory } = await setup();
    transport.failingDispatches = 5;

    await Promise.all(
      Array.from({ length: 20 }, async (_, i) => {
        const session = await factory.open();
        await session.publish(new OrderPlaced(`order-${String(i)}`));
        await session.commit();
      }),
    );
    await transport.drain();

    const ids = new Set(transport.dispatched.map((op) => op.messageId));
    expect(ids.size).toBe(20);
  });
});
