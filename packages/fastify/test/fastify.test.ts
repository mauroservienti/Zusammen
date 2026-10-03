import Fastify, { type FastifyInstance } from 'fastify';
import { describe, expect, test } from 'vitest';
import { createSessionFactory, getSession } from '@zusammen/core';
import { sessionOf, zusammen, type ZusammenFastifyOptions } from '@zusammen/fastify';
import { InMemoryPersistence, InMemoryTransport, type InMemoryTransaction } from '@zusammen/testing';

class OrderPlaced {
  constructor(readonly orderId: string) {}
}

async function setup(routes: (app: FastifyInstance) => void, options: Omit<ZusammenFastifyOptions, 'factory'> = {}) {
  const persistence = new InMemoryPersistence();
  const transport = new InMemoryTransport();
  const factory = createSessionFactory({ persistence, transport });
  await factory.start();
  const app = Fastify();
  await app.register(zusammen, { factory, ...options });
  routes(app);
  await app.ready();
  return { app, persistence, transport };
}

async function placeOrder(orderId: string) {
  const session = getSession<InMemoryTransaction>();
  session.transactionContext.writes.set(orderId, 'placed');
  await session.publish(new OrderPlaced(orderId));
}

const transactional = { config: { transactionalSession: true } };

describe('fastify', () => {
  test('commits before the response is sent', async () => {
    const timeline: string[] = [];
    const { app, persistence, transport } = await setup((app) => {
      app.post<{ Params: { id: string } }>('/orders/:id', transactional, async (request, reply) => {
        await placeOrder(request.params.id);
        return reply.code(201).send({ id: request.params.id });
      });
      app.addHook('onResponse', (_request, _reply, done) => {
        timeline.push('response');
        done();
      });
    });
    const commit = persistence.commit.bind(persistence);
    persistence.commit = async (context) => {
      await commit(context);
      timeline.push('committed');
    };

    const response = await app.inject({ method: 'POST', url: '/orders/o1' });

    expect(response.statusCode).toBe(201);
    expect(response.json()).toEqual({ id: 'o1' });
    expect(timeline).toEqual(['committed', 'response']);
    expect(persistence.data.get('o1')).toBe('placed');
    expect(transport.dispatched).toHaveLength(1);
  });

  test('rolls back when the handler throws', async () => {
    const { app, persistence, transport } = await setup((app) => {
      app.post<{ Params: { id: string } }>('/orders/:id', transactional, async (request) => {
        await placeOrder(request.params.id);
        throw new Error('out of stock');
      });
    });

    const response = await app.inject({ method: 'POST', url: '/orders/o1' });

    expect(response.statusCode).toBe(500);
    expect(persistence.data.size).toBe(0);
    expect(transport.controlQueue).toHaveLength(0);
  });

  test('rolls back on client errors', async () => {
    const { app, persistence } = await setup((app) => {
      app.post<{ Params: { id: string } }>('/orders/:id', transactional, async (request, reply) => {
        await placeOrder(request.params.id);
        return reply.code(409).send({ error: 'duplicate' });
      });
    });

    const response = await app.inject({ method: 'POST', url: '/orders/o1' });

    expect(response.statusCode).toBe(409);
    expect(persistence.data.size).toBe(0);
  });

  test('a failed commit becomes an error response', async () => {
    const { app, persistence } = await setup((app) => {
      app.post<{ Params: { id: string } }>('/orders/:id', transactional, async (request, reply) => {
        await placeOrder(request.params.id);
        return reply.code(201).send({ id: request.params.id });
      });
    });
    persistence.commit = () => Promise.reject(new Error('commit window exceeded'));

    const response = await app.inject({ method: 'POST', url: '/orders/o1' });

    expect(response.statusCode).toBe(500);
    expect(response.json()).toMatchObject({ message: 'commit window exceeded' });
  });

  test('only routes that opt in get a session, unless global', async () => {
    const routes = (app: FastifyInstance) => {
      app.get('/plain', (request, reply) => reply.send({ session: request.transactionalSession !== undefined }));
    };

    const optIn = await setup(routes);
    expect((await optIn.app.inject('/plain')).json()).toEqual({ session: false });

    const global = await setup(routes, { global: true });
    expect((await global.app.inject('/plain')).json()).toEqual({ session: true });
  });

  test('the handler can settle the session itself', async () => {
    const { app, persistence } = await setup((app) => {
      app.post<{ Params: { id: string } }>('/orders/:id', transactional, async (request, reply) => {
        await placeOrder(request.params.id);
        await sessionOf(request).rollback();
        return reply.code(202).send();
      });
    });

    expect((await app.inject({ method: 'POST', url: '/orders/o1' })).statusCode).toBe(202);
    expect(persistence.data.size).toBe(0);
  });

  test('concurrent requests keep their own sessions', async () => {
    const { app, persistence } = await setup((app) => {
      app.post<{ Params: { id: string } }>('/orders/:id', transactional, async (request) => {
        await new Promise((resolve) => setTimeout(resolve, Math.random() * 20));
        expect(getSession()).toBe(request.transactionalSession);
        await placeOrder(request.params.id);
        return {};
      });
    });

    await Promise.all(['a', 'b', 'c'].map((id) => app.inject({ method: 'POST', url: `/orders/${id}` })));

    expect([...persistence.data.keys()].sort()).toEqual(['a', 'b', 'c']);
  });
});
