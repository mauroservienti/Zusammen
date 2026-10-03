// Sales API: stores an order, publishes OrderPlaced and sends ChargeCustomer to the NServiceBus Billing endpoint,
// all in one transactional session. Properties are sent in PascalCase (OrderId, Amount) for System.Text.Json.
import express from 'express';
import { MongoClient, type ClientSession } from 'mongodb';
import { createSessionFactory } from '@zusammen/core';
import { sessionOf, transactionalSession } from '@zusammen/express';
import { MongoDBPersistence } from '@zusammen/mongodb';
import { nserviceBusConvention } from '@zusammen/nservicebus';
import { RabbitMQTransport } from '@zusammen/rabbitmq';
import { nserviceBusConventionalTopology } from '@zusammen/rabbitmq/nservicebus';
import { ChargeCustomer, OrderPlaced } from './messages.ts';

const client = new MongoClient(process.env.MONGO_URL ?? 'mongodb://localhost:27017/sales?replicaSet=rs0');
const orders = client.db().collection<{ _id: string; amount: number }>('orders');

const factory = createSessionFactory({
  persistence: new MongoDBPersistence({ client }),
  transport: new RabbitMQTransport({
    url: process.env.AMQP_URL ?? 'amqp://localhost',
    controlQueue: 'sales-api.control',
    topology: nserviceBusConventionalTopology(),
  }),
  convention: nserviceBusConvention({
    endpointName: 'Sales.Api',
    messageTypes: new Map([[OrderPlaced, 'Sales.Messages.OrderPlaced']]),
    // Conventional topology: events go to the exchange NServiceBus subscribers bind to
    topics: { 'Sales.Messages.OrderPlaced': 'Sales.Messages:OrderPlaced' },
  }),
  // Convenient for a sample; in production, create resources as part of the deployment
  createResources: true,
  logger: console,
});
await client
  .db()
  .createCollection('orders')
  .catch(() => undefined);
await factory.start();

const app = express();
app.use(express.json());
app.post('/orders', transactionalSession(factory), async (request, response) => {
  const { id, amount } = request.body as { id: string; amount: number };
  const session = sessionOf<ClientSession>(request);

  await orders.insertOne({ _id: id, amount }, { session: session.transactionContext });
  await session.publish(new OrderPlaced(id));
  await session.send('Billing', new ChargeCustomer(id, amount));

  response.status(201).json({ id });
});

const server = app.listen(3001, () => {
  console.log('Sales API listening on http://localhost:3001');
});

process.once('SIGINT', () => {
  server.close();
  void factory.stop().then(() => client.close());
});
