import { Hono } from 'hono';
import { describe, expect, test } from 'vitest';
import { createSessionFactory, getSession, type SessionFactory } from '@zusammen/core';
import { sessionOf, transactionalSession, type TransactionalSessionVariables } from '@zusammen/hono';
import { InMemoryPersistence, InMemoryTransport, type InMemoryTransaction } from '@zusammen/testing';

class OrderPlaced {
  constructor(readonly orderId: string) {}
}

type App = Hono<{ Variables: TransactionalSessionVariables }>;

async function setup(routes: (app: App, factory: SessionFactory<InMemoryTransaction>) => void) {
  const persistence = new InMemoryPersistence();
  const transport = new InMemoryTransport();
  const factory = createSessionFactory({ persistence, transport });
  await factory.start();
  const app: App = new Hono();
  routes(app, factory);
  return { app, persistence, transport };
}

async function placeOrder(orderId: string) {
  const session = getSession<InMemoryTransaction>();
  session.transactionContext.writes.set(orderId, 'placed');
  await session.publish(new OrderPlaced(orderId));
}

describe('hono', () => {
  test('commits before the response is returned', async () => {
    const { app, persistence, transport } = await setup((app, factory) => {
      app.post('/orders/:id', transactionalSession(factory), async (c) => {
        await placeOrder(c.req.param('id'));
        return c.json({ id: c.req.param('id') }, 201);
      });
    });

    const response = await app.request('/orders/o1', { method: 'POST' });

    expect(response.status).toBe(201);
    expect(await response.json()).toEqual({ id: 'o1' });
    expect(persistence.data.get('o1')).toBe('placed');
    expect(transport.dispatched).toHaveLength(1);
  });

  test('rolls back when the handler throws', async () => {
    const { app, persistence, transport } = await setup((app, factory) => {
      app.post('/orders/:id', transactionalSession(factory), async (c) => {
        await placeOrder(c.req.param('id'));
        throw new Error('out of stock');
      });
    });

    const response = await app.request('/orders/o1', { method: 'POST' });

    expect(response.status).toBe(500);
    expect(persistence.data.size).toBe(0);
    expect(transport.controlQueue).toHaveLength(0);
  });

  test('rolls back on client errors', async () => {
    const { app, persistence } = await setup((app, factory) => {
      app.post('/orders/:id', transactionalSession(factory), async (c) => {
        await placeOrder(c.req.param('id'));
        return c.json({ error: 'duplicate' }, 409);
      });
    });

    expect((await app.request('/orders/o1', { method: 'POST' })).status).toBe(409);
    expect(persistence.data.size).toBe(0);
  });

  test('a failed commit replaces the response, including its headers', async () => {
    const { app, persistence } = await setup((app, factory) => {
      app.post('/orders/:id', transactionalSession(factory), async (c) => {
        await placeOrder(c.req.param('id'));
        c.header('x-order', 'o1');
        return c.json({ id: 'o1' }, 201);
      });
    });
    persistence.commit = () => Promise.reject(new Error('commit window exceeded'));

    const response = await app.request('/orders/o1', { method: 'POST' });

    expect(response.status).toBe(500);
    expect(response.headers.get('x-order')).toBeNull();
    expect(await response.text()).toBe('');
  });

  test('custom commit error responses', async () => {
    const { app, persistence } = await setup((app, factory) => {
      app.post(
        '/orders',
        transactionalSession(factory, { onCommitError: (_error, c) => c.json({ retry: true }, 503) }),
        (c) => c.json({}),
      );
    });
    persistence.commit = () => Promise.reject(new Error('down'));

    const response = await app.request('/orders', { method: 'POST' });

    expect(response.status).toBe(503);
    expect(await response.json()).toEqual({ retry: true });
  });

  test('the handler can settle the session itself', async () => {
    const { app, persistence } = await setup((app, factory) => {
      app.post('/orders/:id', transactionalSession(factory), async (c) => {
        await placeOrder(c.req.param('id'));
        await sessionOf(c).rollback();
        return c.body(null, 202);
      });
    });

    expect((await app.request('/orders/o1', { method: 'POST' })).status).toBe(202);
    expect(persistence.data.size).toBe(0);
  });

  test('concurrent requests keep their own sessions', async () => {
    const { app, persistence } = await setup((app, factory) => {
      app.post('/orders/:id', transactionalSession(factory), async (c) => {
        await new Promise((resolve) => setTimeout(resolve, Math.random() * 20));
        expect(getSession()).toBe(sessionOf(c));
        await placeOrder(c.req.param('id'));
        return c.json({});
      });
    });

    await Promise.all(['a', 'b', 'c'].map(async (id) => app.request(`/orders/${id}`, { method: 'POST' })));

    expect([...persistence.data.keys()].sort()).toEqual(['a', 'b', 'c']);
  });
});
