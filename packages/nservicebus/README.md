# @zusammen/nservicebus

Opt-in NServiceBus wire compatibility for [Zusammen](https://github.com/mauroservienti/Zusammen): messages that [NServiceBus](https://particular.net/nservicebus) endpoints using the RabbitMQ transport can consume, following the [native integration](https://docs.particular.net/transports/rabbitmq/native-integration) guidance.

## Install

```sh
npm install @zusammen/core @zusammen/nservicebus @zusammen/rabbitmq amqplib
```

Until 1.0, releases are prereleases on the `next` dist-tag: append `@next` to the `@zusammen/*` packages to get the latest one.

## Usage

```typescript
import { createSessionFactory } from '@zusammen/core';
import { nserviceBusConvention } from '@zusammen/nservicebus';
import { RabbitMQTransport } from '@zusammen/rabbitmq';
import { nserviceBusConventionalTopology } from '@zusammen/rabbitmq/nservicebus';

const factory = createSessionFactory({
  persistence,
  transport: new RabbitMQTransport({ url, topology: nserviceBusConventionalTopology() }),
  convention: nserviceBusConvention({
    endpointName: 'Sales.Api',
    messageTypes: new Map([[PlaceOrder, 'Sales.Messages.PlaceOrder']]),
    topics: { OrderPlaced: 'Sales.Messages:OrderPlaced' },
  }),
});

await session.send('Sales', new PlaceOrder('o1')); // to the NServiceBus endpoint "Sales"
await session.publish(new OrderPlaced('o1')); // to subscribers of Sales.Messages.OrderPlaced
```

The NServiceBus topologies reject other conventions at startup. Use `nserviceBusDirectTopology()` for endpoints on the direct routing topology.

## Options

| Option           | Default   | Description                                                                                     |
| ---------------- | --------- | ----------------------------------------------------------------------------------------------- |
| `endpointName`   |           | Sent as `NServiceBus.OriginatingEndpoint`                                                       |
| `messageTypes`   |           | .NET FullNames, keyed by constructor or class name                                              |
| `topics`         |           | Publish topics keyed by message type name (exchange for conventional, routing key for direct)   |
| `replyToAddress` |           | Sent as `NServiceBus.ReplyToAddress`                                                            |
| `propertyNaming` | `pascal`  | `pascal` for NServiceBus' default System.Text.Json settings, `preserve` for camelCase receivers |
| `serializer`     | JSON      | Custom serializer; its content type is sent as `NServiceBus.ContentType`                        |
| `machineName`    | host name | Sent as `NServiceBus.OriginatingMachine`                                                        |

## Wire format

Headers: `NServiceBus.MessageId`, `NServiceBus.MessageIntent` (`Send` / `Publish`), `NServiceBus.TimeSent` (`yyyy-MM-dd HH:mm:ss:ffffff Z`), `NServiceBus.ContentType`, `NServiceBus.ConversationId` (default: the session ID), `NServiceBus.CorrelationId` (default: the message ID), `NServiceBus.OriginatingEndpoint`, `NServiceBus.OriginatingMachine`, optionally `NServiceBus.ReplyToAddress`, plus `zusammen.message-type`. `NServiceBus.EnclosedMessageTypes` is set for mapped types only.

**Unmapped types** need the [`Zusammen.NServiceBus`](../../dotnet) package on the receiving endpoint. See [message types](../../docs/guides/message-types.md) for topics, mapping and inheritance, and [delivery guarantees](../../docs/guides/delivery-guarantees.md) for using the NServiceBus outbox to deduplicate.
