import type { AddressInfo } from 'node:net';
import express, { type Express } from 'express';
import { afterEach, describe, expect, test } from 'vitest';
import { createSessionFactory, getSession, type SessionFactory } from '@zusammen/core';
import { sessionOf, transactionalSession } from '@zusammen/express';
import { InMemoryPersistence, InMemoryTransport, type InMemoryTransaction } from '@zusammen/testing';

class OrderPlaced {
  constructor(readonly orderId: string) {}
}

const servers: { close(): void }[] = [];
afterEach(() => {
  for (const server of servers.splice(0)) server.close();
});

async function setup(configure: (app: Express, factory: SessionFactory<InMemoryTransaction>) => void) {
  const persistence = new InMemoryPersistence();
  const transport = new InMemoryTransport();
  const factory = createSessionFactory({ persistence, transport });
  await factory.start();
  const app = express();
  app.use(express.json());
  configure(app, factory);
  const server = app.listen(0);
  servers.push(server);
  await new Promise((resolve) => server.once('listening', resolve));
  const url = `http://localhost:${String((server.address() as AddressInfo).port)}`;
  return { persistence, transport, url };
}

// Business code that doesn't know about HTTP
async function placeOrder(orderId: string) {
  const session = getSession<InMemoryTransaction>();
  session.transactionContext.writes.set(orderId, 'placed');
  await session.publish(new OrderPlaced(orderId));
}

describe('express', () => {
  test('commits before the response reaches the client', async () => {
    const timeline: string[] = [];
    const { persistence, transport, url } = await setup((app, factory) => {
      app.post('/orders/:id', transactionalSession(factory), async (request, response) => {
        await placeOrder(request.params.id as string);
        response.status(201).json({ id: request.params.id });
      });
    });
    const commit = persistence.commit.bind(persistence);
    persistence.commit = async (context) => {
      await new Promise((resolve) => setTimeout(resolve, 100));
      await commit(context);
      timeline.push('committed');
    };

    const response = await fetch(`${url}/orders/o1`, { method: 'POST' });
    timeline.push('response');

    expect(response.status).toBe(201);
    expect(await response.json()).toEqual({ id: 'o1' });
    expect(timeline).toEqual(['committed', 'response']);
    expect(persistence.data.get('o1')).toBe('placed');
    expect(transport.dispatched).toHaveLength(1);
  });

  test('rolls back when the handler throws', async () => {
    const { persistence, transport, url } = await setup((app, factory) => {
      app.post('/orders/:id', transactionalSession(factory), async (request) => {
        await placeOrder(request.params.id as string);
        throw new Error('out of stock');
      });
    });

    const response = await fetch(`${url}/orders/o1`, { method: 'POST' });

    expect(response.status).toBe(500);
    expect(persistence.data.size).toBe(0);
    expect(transport.controlQueue).toHaveLength(0);
  });

  test('rolls back on client errors', async () => {
    const { persistence, url } = await setup((app, factory) => {
      app.post('/orders/:id', transactionalSession(factory), async (request, response) => {
        await placeOrder(request.params.id as string);
        response.status(409).json({ error: 'duplicate' });
      });
    });

    const response = await fetch(`${url}/orders/o1`, { method: 'POST' });

    expect(response.status).toBe(409);
    expect(await response.json()).toEqual({ error: 'duplicate' });
    expect(persistence.data.size).toBe(0);
  });

  test('a failed commit becomes a 500 without the handler headers', async () => {
    const { persistence, url } = await setup((app, factory) => {
      app.post('/orders/:id', transactionalSession(factory), async (request, response) => {
        await placeOrder(request.params.id as string);
        response.status(201).set('x-order', 'o1').json({ id: 'o1' });
      });
    });
    persistence.commit = () => Promise.reject(new Error('commit window exceeded'));

    const response = await fetch(`${url}/orders/o1`, { method: 'POST' });

    expect(response.status).toBe(500);
    expect(response.headers.get('x-order')).toBeNull();
    expect(await response.text()).toBe('');
  });

  test('custom commit error responses', async () => {
    const { persistence, url } = await setup((app, factory) => {
      app.post(
        '/orders',
        transactionalSession(factory, {
          onCommitError: (_error, _request, response) => response.status(503).json({ retry: true }),
        }),
        (_request, response) => {
          response.json({});
        },
      );
    });
    persistence.commit = () => Promise.reject(new Error('down'));

    const response = await fetch(`${url}/orders`, { method: 'POST' });

    expect(response.status).toBe(503);
    expect(await response.json()).toEqual({ retry: true });
  });

  test('streamed responses are delivered completely after the commit', async () => {
    const { persistence, url } = await setup((app, factory) => {
      app.get('/report', transactionalSession(factory), (_request, response) => {
        sessionOf<InMemoryTransaction>(_request).transactionContext.writes.set('report', 'generated');
        response.write('a');
        response.write('b');
        response.end('c');
      });
    });

    const response = await fetch(`${url}/report`);

    expect(await response.text()).toBe('abc');
    expect(persistence.data.get('report')).toBe('generated');
  });

  test('the handler can settle the session itself', async () => {
    const { persistence, url } = await setup((app, factory) => {
      app.post('/orders/:id', transactionalSession(factory), async (request, response) => {
        await placeOrder(request.params.id as string);
        await sessionOf(request).rollback();
        response.status(202).end();
      });
    });

    const response = await fetch(`${url}/orders/o1`, { method: 'POST' });

    expect(response.status).toBe(202);
    expect(persistence.data.size).toBe(0);
  });

  test('routes without the middleware have no session', async () => {
    const { url } = await setup((app) => {
      app.get('/health', (request, response) => {
        response.json({ session: request.transactionalSession !== undefined });
      });
    });

    expect(await (await fetch(`${url}/health`)).json()).toEqual({ session: false });
  });

  test('aborted requests roll back', async () => {
    let closed!: () => void;
    const aborted = new Promise<void>((resolve) => (closed = resolve));
    const { persistence, url } = await setup((app, factory) => {
      app.post('/slow', transactionalSession(factory), async (request, response) => {
        sessionOf<InMemoryTransaction>(request).transactionContext.writes.set('slow', 'started');
        response.once('close', closed);
        await new Promise((resolve) => setTimeout(resolve, 2_000));
        response.json({});
      });
    });

    const controller = new AbortController();
    const request = fetch(`${url}/slow`, { method: 'POST', signal: controller.signal });
    await new Promise((resolve) => setTimeout(resolve, 100));
    controller.abort();
    await expect(request).rejects.toThrow();
    await aborted;
    await new Promise((resolve) => setTimeout(resolve, 50));

    expect(persistence.data.size).toBe(0);
  });
});
