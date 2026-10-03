// Orders API: stores an order and publishes OrderPlaced in one transactional session.
import express from 'express';
import { MongoClient, MongoServerError, type ClientSession } from 'mongodb';
import { createSessionFactory, getSession } from '@zusammen/core';
import { transactionalSession } from '@zusammen/express';
import { MongoDBPersistence } from '@zusammen/mongodb';
import { RabbitMQTransport } from '@zusammen/rabbitmq';
import { amqpUrl, mongoUrl } from './config.ts';
import { OrderPlaced } from './messages.ts';

const client = new MongoClient(mongoUrl);
const orders = client.db().collection<{ _id: string; total: number }>('orders');

const factory = createSessionFactory({
  persistence: new MongoDBPersistence({ client }),
  transport: new RabbitMQTransport({ url: amqpUrl, controlQueue: 'orders-api.control' }),
  // Convenient for a sample; in production, create resources as part of the deployment
  createResources: true,
  logger: console,
});
await client
  .db()
  .createCollection('orders')
  .catch(() => undefined);
await factory.start();

// Business logic doesn't need to know about HTTP or sessions being passed around
async function placeOrder(orderId: string, total: number) {
  const session = getSession<ClientSession>();
  await orders.insertOne({ _id: orderId, total }, { session: session.transactionContext });
  await session.publish(new OrderPlaced(orderId, total));
}

const app = express();
app.use(express.json());
app.post('/orders', transactionalSession(factory), async (request, response) => {
  const { id, total } = request.body as { id: string; total: number };
  try {
    await placeOrder(id, total);
  } catch (error) {
    if (error instanceof MongoServerError && error.code === 11000) {
      // Error responses roll the session back: no order, no event
      response.status(409).json({ error: `Order ${id} already exists` });
      return;
    }
    throw error;
  }
  // Sent only after the order and the event are committed
  response.status(201).json({ id });
});

const server = app.listen(3000, () => {
  console.log('Orders API listening on http://localhost:3000');
});

process.once('SIGINT', () => {
  server.close();
  void factory.stop().then(() => client.close());
});
