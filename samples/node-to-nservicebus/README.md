# Sample: Node.js → NServiceBus

A Sales API in Node.js stores an order, publishes `OrderPlaced` and sends `ChargeCustomer` to an NServiceBus endpoint, all in one transactional session. The Billing endpoint handles both.

- `src/api.ts`: Express API with the NServiceBus convention and the conventional routing topology.
  - `OrderPlaced` is **mapped** to its .NET type (`Sales.Messages.OrderPlaced`), so it carries `NServiceBus.EnclosedMessageTypes` and works with any NServiceBus endpoint. Its topic is the exchange NServiceBus subscribers bind to, `Sales.Messages:OrderPlaced`.
  - `ChargeCustomer` is **not mapped**: it carries only `zusammen.message-type: ChargeCustomer`.
- `Billing/`: NServiceBus 10 endpoint on the RabbitMQ transport, with `EnableZusammen()` from [`Zusammen.NServiceBus`](../../dotnet) to resolve `ChargeCustomer` to `Billing.Messages.ChargeCustomer`.

## Run

Requires the .NET 10 SDK. From the repository root:

```sh
docker compose up -d        # MongoDB replica set + RabbitMQ (with the management API NServiceBus uses)
pnpm install
pnpm build

pnpm --filter @zusammen/sample-node-to-nservicebus billing   # terminal 1: wait until the endpoint started
pnpm --filter @zusammen/sample-node-to-nservicebus api       # terminal 2

curl -X POST localhost:3001/orders -H 'content-type: application/json' -d '{"id":"o1","amount":42.5}'
```

Billing logs `Order o1 placed, preparing invoice` and `Charging 42.5 for order o1`.

Start Billing first: it creates its queue and subscribes to `OrderPlaced`, creating the `Sales.Messages:OrderPlaced` exchange. Events published before anyone subscribed go nowhere, as with any NServiceBus publisher.

In production, enable the [NServiceBus outbox](https://docs.particular.net/nservicebus/outbox/) on Billing: Zusammen delivers at least once, and the outbox deduplicates by message ID.
