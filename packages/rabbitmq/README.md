# @zusammen/rabbitmq

RabbitMQ transport for [Zusammen](https://github.com/mauroservienti/Zusammen) transactional sessions, on amqplib 2.

## Usage

```typescript
import { RabbitMQTransport } from '@zusammen/rabbitmq';

const transport = new RabbitMQTransport({ url: 'amqp://localhost', controlQueue: 'sales.control' });
const factory = createSessionFactory({ persistence, transport });
await factory.start();
```

## Options

| Option          | Default              | Description                                          |
| --------------- | -------------------- | ---------------------------------------------------- |
| `url`           |                      | Connection URL or amqplib connect options            |
| `socketOptions` |                      | amqplib socket options                               |
| `recovery`      | amqplib defaults     | Reconnection backoff; reconnection is always on      |
| `topology`      | `zusammenTopology()` | Where messages go                                    |
| `controlQueue`  | `zusammen.control`   | One per application sharing a broker                 |
| `queueType`     | `quorum`             | Type of the control and error queues                 |
| `prefetch`      | `10`                 | Control messages processed concurrently per instance |
| `logger`        | silent               |                                                      |

## Topologies

**`zusammenTopology()`** (default): sends go through the default exchange to the queue named after the destination; publishes go to the durable topic exchange `zusammen.events`, with the `topic` or the message type as routing key. Subscribers bind their own queues:

```typescript
await channel.bindQueue('shipping', 'zusammen.events', 'OrderPlaced');
```

**NServiceBus**: `nserviceBusConventionalTopology()` and `nserviceBusDirectTopology()` from `@zusammen/rabbitmq/nservicebus`, used with [`@zusammen/nservicebus`](../nservicebus).

## Delivery

- Every publish waits for the broker's confirm.
- Sends are `mandatory`: a send nobody can receive fails with `UnroutableMessageError` and is retried by the control message. Publishes aren't: events without subscribers are fine.
- Messages are persistent, with `message_id`, `type`, `content_type`, `correlation_id`, `reply_to` and headers set from the outgoing message.

## Resources

`createResources()` declares the topology's exchanges, the control and error queues, and the delay queues `<control>.delay.{1,2,4,8,16,32,64}s` (classic queues with a queue-level TTL dead-lettering back to the control queue). Delays round up to the next level. Nothing is declared on reconnect. See [operations](../../docs/guides/operations.md).
