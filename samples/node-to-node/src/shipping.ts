// Shipping worker: receives OrderPlaced and creates a shipment, once per message even if it's delivered twice.
import amqp from 'amqplib';
import { MongoClient, MongoServerError } from 'mongodb';
import { ZUSAMMEN_EVENTS_EXCHANGE } from '@zusammen/rabbitmq';
import { amqpUrl, mongoUrl } from './config.ts';

const client = new MongoClient(mongoUrl);
const database = client.db();
const shipments = database.collection<{ _id: string; total: number }>('shipments');
// Message IDs already handled: Zusammen delivers at least once, so receivers deduplicate
const processed = database.collection<{ _id: string }>('shipping_processed_messages');
await database.createCollection('shipments').catch(() => undefined);
await database.createCollection('shipping_processed_messages').catch(() => undefined);

const connection = await amqp.connect(amqpUrl);
const channel = await connection.createChannel();
await channel.assertExchange(ZUSAMMEN_EVENTS_EXCHANGE, 'topic', { durable: true });
await channel.assertQueue('shipping', { durable: true });
await channel.bindQueue('shipping', ZUSAMMEN_EVENTS_EXCHANGE, 'OrderPlaced');
await channel.prefetch(10);

await channel.consume('shipping', (message) => {
  if (message === null) return;
  const messageId = String(message.properties.messageId);
  const event = JSON.parse(message.content.toString()) as { orderId: string; total: number };

  const session = client.startSession();
  session
    .withTransaction(async () => {
      try {
        await processed.insertOne({ _id: messageId }, { session });
      } catch (error) {
        if (error instanceof MongoServerError && error.code === 11000) {
          console.log(`Duplicate of ${messageId}, already shipped`);
          return;
        }
        throw error;
      }
      await shipments.insertOne({ _id: event.orderId, total: event.total }, { session });
      console.log(`Shipping order ${event.orderId}`);
    })
    .then(
      () => {
        channel.ack(message);
      },
      (error: unknown) => {
        console.error('Failed, retrying', error);
        channel.nack(message, false, true);
      },
    )
    .finally(() => void session.endSession());
});

console.log('Shipping worker waiting for OrderPlaced events');
process.once('SIGINT', () => {
  void connection.close().then(() => client.close());
});
