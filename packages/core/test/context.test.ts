import { describe, expect, test } from 'vitest';
import {
  createSessionFactory,
  getSession,
  NoActiveSessionError,
  settleSession,
  tryGetSession,
  withSession,
} from '@zusammen/core';
import { InMemoryPersistence, InMemoryTransport, type InMemoryTransaction } from '@zusammen/testing';

class OrderPlaced {
  constructor(readonly orderId: string) {}
}

async function setup() {
  const persistence = new InMemoryPersistence();
  const transport = new InMemoryTransport();
  const factory = createSessionFactory({ persistence, transport });
  await factory.start();
  return { persistence, transport, factory };
}

// Code deep in the call stack, unaware of sessions being passed around
async function placeOrder(orderId: string) {
  const session = getSession<InMemoryTransaction>();
  session.transactionContext.writes.set(orderId, 'placed');
  await session.publish(new OrderPlaced(orderId));
}

describe('withSession', () => {
  test('commits when the function succeeds, with the session reachable anywhere in the call stack', async () => {
    const { persistence, transport, factory } = await setup();

    const result = await withSession(factory, async () => {
      await placeOrder('o1');
      return 'done';
    });

    expect(result).toBe('done');
    expect(persistence.data.get('o1')).toBe('placed');
    expect(transport.dispatched).toHaveLength(1);
    expect(tryGetSession()).toBeUndefined();
  });

  test('rolls back when the function throws', async () => {
    const { persistence, transport, factory } = await setup();

    await expect(
      withSession(factory, async () => {
        await placeOrder('o1');
        throw new Error('validation failed');
      }),
    ).rejects.toThrow('validation failed');

    expect(persistence.data.size).toBe(0);
    expect(transport.controlQueue).toHaveLength(0);
  });

  test('leaves sessions the function settled itself', async () => {
    const { persistence, factory } = await setup();

    await withSession(factory, async (session) => {
      await placeOrder('o1');
      await session.rollback();
    });

    expect(persistence.data.size).toBe(0);
  });

  test('concurrent sessions stay isolated', async () => {
    const { persistence, factory } = await setup();

    await Promise.all(
      ['a', 'b', 'c'].map((id) =>
        withSession(factory, async (session) => {
          await new Promise((resolve) => setTimeout(resolve, Math.random() * 20));
          expect(getSession()).toBe(session);
          await placeOrder(id);
        }),
      ),
    );

    expect([...persistence.data.keys()].sort()).toEqual(['a', 'b', 'c']);
  });

  test('getSession outside a session throws', () => {
    expect(() => getSession()).toThrow(NoActiveSessionError);
  });
});

describe('settleSession', () => {
  test('commits on success, rolls back on failure, ignores settled sessions', async () => {
    const { persistence, factory } = await setup();

    const committed = await factory.open();
    committed.transactionContext.writes.set('ok', 'yes');
    await settleSession(committed, true);

    const rolledBack = await factory.open();
    rolledBack.transactionContext.writes.set('failed', 'yes');
    await settleSession(rolledBack, false);
    await settleSession(rolledBack, true);

    expect([...persistence.data.keys()]).toEqual(['ok']);
    expect(committed.status).toBe('committed');
    expect(rolledBack.status).toBe('rolledBack');
  });
});
