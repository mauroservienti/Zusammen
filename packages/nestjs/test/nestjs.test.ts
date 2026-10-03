import 'reflect-metadata';
import {
  Controller,
  Get,
  HttpCode,
  Inject,
  Injectable,
  Module,
  Param,
  Post,
  ConflictException,
  type INestApplication,
} from '@nestjs/common';
import { Test } from '@nestjs/testing';
import { afterEach, describe, expect, test } from 'vitest';
import { createSessionFactory, getSession, type TransactionalSession } from '@zusammen/core';
import { CurrentSession, SessionAccessor, Transactional, ZusammenModule } from '@zusammen/nestjs';
import { InMemoryPersistence, InMemoryTransport, type InMemoryTransaction } from '@zusammen/testing';

class OrderPlaced {
  constructor(readonly orderId: string) {}
}

@Injectable()
class OrderService {
  constructor(@Inject(SessionAccessor) private readonly sessions: SessionAccessor) {}

  async place(orderId: string) {
    const session = this.sessions.current as TransactionalSession<InMemoryTransaction>;
    session.transactionContext.writes.set(orderId, 'placed');
    await session.publish(new OrderPlaced(orderId));
  }
}

@Controller('orders')
class OrdersController {
  constructor(@Inject(OrderService) private readonly orders: OrderService) {}

  @Post(':id')
  @Transactional()
  async place(@Param('id') id: string) {
    await this.orders.place(id);
    return { id };
  }

  @Post(':id/fail')
  @Transactional()
  async fail(@Param('id') id: string) {
    await this.orders.place(id);
    throw new ConflictException('duplicate');
  }

  @Post(':id/self-managed')
  @Transactional()
  @HttpCode(202)
  async selfManaged(@Param('id') id: string, @CurrentSession() session: TransactionalSession<unknown>) {
    await this.orders.place(id);
    await session.rollback();
  }

  @Get('health')
  health() {
    return { session: getSessionIfAny() !== undefined };
  }
}

function getSessionIfAny() {
  try {
    return getSession();
  } catch {
    return undefined;
  }
}

const apps: INestApplication[] = [];
afterEach(async () => {
  await Promise.all(apps.splice(0).map((app) => app.close()));
});

async function setup(options: { global?: boolean } = {}) {
  const persistence = new InMemoryPersistence();
  const transport = new InMemoryTransport();
  const factory = createSessionFactory({ persistence, transport });

  @Module({
    imports: [ZusammenModule.forRoot({ factory, ...options })],
    controllers: [OrdersController],
    providers: [OrderService],
  })
  // eslint-disable-next-line @typescript-eslint/no-extraneous-class -- Nest module
  class AppModule {}

  const moduleRef = await Test.createTestingModule({ imports: [AppModule] }).compile();
  const app = moduleRef.createNestApplication({ logger: false });
  apps.push(app);
  await app.listen(0);
  const url = await app.getUrl();
  return { persistence, transport, url: url.replace('[::1]', 'localhost') };
}

describe('nestjs', () => {
  test('starts the factory with the application and commits transactional routes', async () => {
    const { persistence, transport, url } = await setup();

    const response = await fetch(`${url}/orders/o1`, { method: 'POST' });

    expect(response.status).toBe(201);
    expect(await response.json()).toEqual({ id: 'o1' });
    expect(persistence.data.get('o1')).toBe('placed');
    expect(transport.dispatched).toHaveLength(1);
  });

  test('rolls back when the handler throws', async () => {
    const { persistence, transport, url } = await setup();

    const response = await fetch(`${url}/orders/o1/fail`, { method: 'POST' });

    expect(response.status).toBe(409);
    expect(persistence.data.size).toBe(0);
    expect(transport.controlQueue).toHaveLength(0);
  });

  test('a failed commit becomes an error response', async () => {
    const { persistence, url } = await setup();
    persistence.commit = () => Promise.reject(new Error('commit window exceeded'));

    const response = await fetch(`${url}/orders/o1`, { method: 'POST' });

    expect(response.status).toBe(500);
  });

  test('the handler can settle the session itself', async () => {
    const { persistence, url } = await setup();

    const response = await fetch(`${url}/orders/o1/self-managed`, { method: 'POST' });

    expect(response.status).toBe(202);
    expect(persistence.data.size).toBe(0);
  });

  test('only marked routes get a session, unless global', async () => {
    const optIn = await setup();
    expect(await (await fetch(`${optIn.url}/orders/health`)).json()).toEqual({ session: false });

    const global = await setup({ global: true });
    expect(await (await fetch(`${global.url}/orders/health`)).json()).toEqual({ session: true });
  });

  test('concurrent requests keep their own sessions', async () => {
    const { persistence, url } = await setup();

    await Promise.all(['a', 'b', 'c'].map((id) => fetch(`${url}/orders/${id}`, { method: 'POST' })));

    expect([...persistence.data.keys()].sort()).toEqual(['a', 'b', 'c']);
  });
});
