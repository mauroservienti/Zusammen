# Message types

Every outgoing message has a **message type name**. It tells receivers what the message is and, depending on the routing topology, decides where published events go. This guide covers how the name is chosen and how to keep it stable.

## Default convention

The default (Zusammen) convention resolves the type name, in order, from:

1. the `messageType` option of `send` / `publish`;
2. the `messageTypes` registry of `zusammenConvention()`;
3. the class name of the message (`message.constructor.name`).

Plain objects (`{ orderId: 'o1' }`) have no class name, so they need option 1 or 2; otherwise `send` / `publish` rejects with `UnknownMessageTypeError` before anything is committed.

```typescript
class OrderPlaced {
  orderId: string;
  constructor(orderId: string) {
    this.orderId = orderId;
  }
}

await session.publish(new OrderPlaced('o1')); // type: OrderPlaced
await session.publish({ orderId: 'o1' }, { messageType: 'OrderPlaced' }); // explicit
```

The type name is sent as the `zusammen.message-type` header and the AMQP `type` property. With the default RabbitMQ topology, published events use it as the routing key on the `zusammen.events` exchange unless a `topic` is given.

## Bundlers and minifiers rename classes

esbuild, terser and similar tools can rename `OrderPlaced` to `t`, silently changing the type name, and with it the routing key, between builds. Either:

- keep class names (`keepNames: true` in esbuild, `keep_classnames` in terser), or
- register types **by constructor**, which survives renaming:

```typescript
const convention = zusammenConvention({
  messageTypes: new Map([
    [OrderPlaced, 'sales.order-placed'],
    [PlaceOrder, 'sales.place-order'],
  ]),
});
```

A registry keyed by class name (`{ OrderPlaced: 'sales.order-placed' }`) is also supported, but is subject to renaming like the class name itself.

## NServiceBus convention

NServiceBus needs the .NET type of every message. With `nserviceBusConvention()`:

- **Mapped messages** (via the `messageType` option or the `messageTypes` registry, using the .NET FullName such as `Sales.Messages.OrderPlaced`) carry `NServiceBus.EnclosedMessageTypes` and work with any NServiceBus endpoint.
- **Unmapped messages** use the class name and carry only `zusammen.message-type`. Endpoints resolve them with the `Zusammen.NServiceBus` package (`endpointConfiguration.EnableZusammen()`), which matches the name to the endpoint's message types by simple name. Without the package, the message fails on the receiver and ends up in its error queue.

```typescript
const convention = nserviceBusConvention({
  endpointName: 'Sales.Api',
  messageTypes: new Map([[PlaceOrder, 'Sales.Messages.PlaceOrder']]),
  topics: { OrderPlaced: 'Sales.Messages:OrderPlaced' },
});
```

Receiving side, for unmapped messages or names that differ between Node.js and .NET:

```csharp
endpointConfiguration.EnableZusammen()
    .MapMessageType<Sales.Messages.OrderPlaced>("OrderPlacedEvent");
```

### Publish topics

The routing topology decides where events go, and NServiceBus subscribers decide where they listen. Zusammen never derives the publish target from the type; pass `topic` per call or register it in `topics` (keyed by the message type name):

| Topology     | Topic                                               | Example                      |
| ------------ | --------------------------------------------------- | ---------------------------- |
| Conventional | Exchange name: .NET namespace, `:`, type name       | `Sales.Messages:OrderPlaced` |
| Direct       | Routing key: .NET FullName with `.` replaced by `-` | `Sales-Messages-OrderPlaced` |

Publishing without a topic rejects with `UnknownPublishTopicError` before commit. A mistyped topic isn't detected: the event goes nowhere, like an event without subscribers.

Base classes and interfaces are out of scope: subscribe to the concrete event type. For the direct topology, if the .NET event type derives from a non-system base class or implements a non-marker interface, NServiceBus prefixes the routing key with the base type keys (`Base-Type.Derived-Type`); use the same key as the topic.

### Property names

NServiceBus' default System.Text.Json serializer matches property names case-sensitively, so the NServiceBus convention writes PascalCase property names (`orderId` → `OrderId`). For endpoints configured with camelCase JSON options, use `propertyNaming: 'preserve'`.
