# Zusammen

Transactional sessions for Node.js: commit business data and outgoing messages **together**, or not at all.

> **Status: work in progress.** The core library and the MongoDB provider are implemented and tested; the RabbitMQ transport is next. Nothing is published to npm yet. See the [implementation plan](docs/node-js-transactional-session-implementation.md).

## The problem

A web request stores an order and publishes `OrderPlaced`. Writing to the database and sending to the broker are two separate operations: if the process crashes, or the broker is unavailable, between the two, the order exists but nobody hears about it — or, the other way around, everybody hears about an order that was never stored.

Zusammen ("together" in German) applies the **outbox pattern**: outgoing messages are stored in the same database transaction as the business data, and dispatched after the commit.

```typescript
await using session = await factory.open();

await orders.insertOne(order, { session: session.transactionContext });
await session.publish(new OrderPlaced(order.id));

await session.commit(); // data and messages, atomically
```

## How it works

Instead of a background poller scanning the outbox, Zusammen uses a **control message**:

1. On `commit()`, a control message is sent to the broker, then the outbox record and business data are committed in one transaction.
2. Messages are dispatched immediately after the commit (best effort).
3. The control message is the safety net: whichever instance receives it checks the outbox and dispatches anything that is still pending — even if the process that committed has died in the meantime.
4. If the transaction never commits, the control message waits for a bounded commit window, then stores a _tombstone_ so a late commit fails instead of leaving messages behind without a safety net.

No polling, no extra infrastructure: the broker delivers the guarantee.

## Guarantees

- **Atomic state change**: business data and outgoing messages are committed together or not at all.
- **At-least-once dispatch**: every committed message is eventually dispatched, possibly more than once.
- **Stable message IDs**: re-dispatched messages are byte-identical, so receivers can deduplicate.

Zusammen does not provide exactly-once delivery; receivers must be idempotent.

## Packages

| Package                                          | Description                                                         | Status      |
| ------------------------------------------------ | ------------------------------------------------------------------- | ----------- |
| `@zusammen/core`                                 | Sessions, control message handling, contracts, default wire format  | Implemented |
| `@zusammen/mongodb`                              | MongoDB persistence                                                 | Implemented |
| `@zusammen/rabbitmq`                             | RabbitMQ transport                                                  | Planned     |
| `@zusammen/rabbitmq/nservicebus`                 | NServiceBus routing topologies for RabbitMQ (opt-in)                | Planned     |
| `@zusammen/nservicebus`                          | NServiceBus wire format, so .NET endpoints can consume the messages | Planned     |
| `@zusammen/express`, `fastify`, `nestjs`, `hono` | Web framework adapters                                              | Planned     |

### NServiceBus interoperability

Messages use a minimal, library-agnostic format by default. Opting into the NServiceBus convention and topology makes them consumable by [NServiceBus](https://particular.net/nservicebus) endpoints using the RabbitMQ transport, following the [native integration](https://docs.particular.net/transports/rabbitmq/native-integration) guidance. Behavior is checked against the [NServiceBus TransactionalSession](https://github.com/Particular/NServiceBus.TransactionalSession) acceptance tests via a [conformance matrix](docs/nservicebus-conformance.md).

## Development

Requirements: Node.js 22.13+, [pnpm](https://pnpm.io), Docker (for local services and integration tests).

```sh
pnpm install
pnpm build        # tsc project references
pnpm test         # vitest, including integration tests (Docker required)
pnpm test:unit    # without integration tests
pnpm typecheck
pnpm lint
pnpm format       # prettier

docker compose up -d   # MongoDB (single-node replica set) and RabbitMQ for local experiments
```

`pnpm conformance:check` verifies the NServiceBus conformance matrix against the upstream acceptance tests.

## License

To be decided.
