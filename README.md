# Zusammen

[![CI](https://github.com/mauroservienti/Zusammen/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/mauroservienti/Zusammen/actions/workflows/ci.yml)
[![npm](https://img.shields.io/npm/v/@zusammen/core/next?label=npm%20%40zusammen%2Fcore)](https://www.npmjs.com/package/@zusammen/core)
[![NuGet](https://img.shields.io/nuget/vpre/Zusammen.NServiceBus?label=NuGet%20Zusammen.NServiceBus)](https://www.nuget.org/packages/Zusammen.NServiceBus)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

Transactional sessions for Node.js: commit business data and outgoing messages **together**, or not at all.

> **Status: alpha.** Everything described here is implemented and tested, including crash and race scenarios end to end and compatibility tests against real NServiceBus endpoints. Prereleases are published to npm (`next` dist-tag) and NuGet; APIs may still change before 1.0. See the [implementation plan](docs/node-js-transactional-session-implementation.md) for the design.

## The problem

A web request stores an order and publishes `OrderPlaced`. Writing to the database and sending to the broker are two separate operations: if the process crashes, or the broker is unavailable, between the two, the order exists but nobody hears about it — or, the other way around, everybody hears about an order that was never stored.

Zusammen ("together" in German) applies the **outbox pattern**: outgoing messages are stored in the same database transaction as the business data, and dispatched after the commit.

```typescript
await using session = await factory.open();

await orders.insertOne(order, { session: session.transactionContext });
await session.publish(new OrderPlaced(order.id));

await session.commit(); // data and messages, atomically
```

## Installation

Pick a persistence, a transport and, optionally, a framework adapter. Libraries the packages integrate with are peer dependencies, installed alongside:

```sh
npm install @zusammen/core @zusammen/mongodb @zusammen/rabbitmq mongodb amqplib
npm install @zusammen/express express   # or fastify, nestjs, hono
npm install @zusammen/nservicebus       # opt-in NServiceBus compatibility
```

Until 1.0, releases are prereleases on the `next` dist-tag: add `@next` (e.g. `npm install @zusammen/core@next`) to get the latest one. Requires Node.js 22.13+ and MongoDB running as a replica set or sharded cluster (transactions). Tested with MongoDB 8, RabbitMQ 4 and NServiceBus 10.

For NServiceBus endpoints receiving unmapped messages: `dotnet add package Zusammen.NServiceBus --prerelease`.

## How it works

Instead of a background poller scanning the outbox, Zusammen uses a **control message**:

1. On `commit()`, a control message is sent to the broker, then the outbox record and business data are committed in one transaction.
2. Messages are dispatched immediately after the commit (best effort).
3. The control message is the safety net: whichever instance receives it checks the outbox and dispatches anything that is still pending — even if the process that committed has died in the meantime.
4. If the transaction never commits, the control message waits for a bounded commit window, then stores a _tombstone_ so a late commit fails instead of leaving messages behind without a safety net.

No polling, no extra infrastructure: the broker delivers the guarantee.

## Infrastructure

Zusammen doesn't create collections, indexes, queues or exchanges unless you ask it to. By default, `start()` verifies that everything it needs exists and fails with a list of what's missing. Either provision resources as part of your deployment (each provider exposes `createResources()` for deployment scripts), or let the factory create them:

```typescript
const factory = createSessionFactory({ persistence, transport, createResources: true });
```

## Web frameworks

Adapters open a session per request, make it available to your code (as a request property and through `getSession()` anywhere in the call stack), and commit **before the response is sent**, so a client never sees success for a commit that failed. Success responses (below 400) commit; errors and thrown exceptions roll back.

```typescript
import { getSession } from '@zusammen/core';
import { transactionalSession } from '@zusammen/express';

app.post('/orders', transactionalSession(factory), async (req, res) => {
  await placeOrder(req.body); // uses getSession() internally
  res.status(201).json({ ok: true });
});
```

| Framework | Package                                 | Usage                                                                          |
| --------- | --------------------------------------- | ------------------------------------------------------------------------------ |
| Express 5 | [`@zusammen/express`](packages/express) | `transactionalSession(factory)` middleware                                     |
| Fastify 5 | [`@zusammen/fastify`](packages/fastify) | `zusammen` plugin; routes opt in with `config: { transactionalSession: true }` |
| NestJS 12 | [`@zusammen/nestjs`](packages/nestjs)   | `ZusammenModule.forRoot({ factory })`, `@Transactional()`, `@CurrentSession()` |
| Hono 4    | [`@zusammen/hono`](packages/hono)       | `transactionalSession(factory)` middleware                                     |

Without a framework, `withSession(factory, async (session) => { … })` commits when the function succeeds and rolls back when it throws.

## Guarantees

- **Atomic state change**: business data and outgoing messages are committed together or not at all.
- **At-least-once dispatch**: every committed message is eventually dispatched, possibly more than once.
- **Stable message IDs**: re-dispatched messages are byte-identical, so receivers can deduplicate.

Zusammen does not provide exactly-once delivery; receivers must be idempotent.

## Packages

| Package                                         | Description                                                                            | Registry                                                     |
| ----------------------------------------------- | -------------------------------------------------------------------------------------- | ------------------------------------------------------------ |
| [`@zusammen/core`](packages/core)               | Sessions, control message handling, contracts, default wire format                     | [npm](https://www.npmjs.com/package/@zusammen/core)          |
| [`@zusammen/mongodb`](packages/mongodb)         | MongoDB persistence                                                                    | [npm](https://www.npmjs.com/package/@zusammen/mongodb)       |
| [`@zusammen/rabbitmq`](packages/rabbitmq)       | RabbitMQ transport; NServiceBus routing topologies in `@zusammen/rabbitmq/nservicebus` | [npm](https://www.npmjs.com/package/@zusammen/rabbitmq)      |
| [`@zusammen/nservicebus`](packages/nservicebus) | Opt-in NServiceBus wire format, so .NET endpoints can consume the messages             | [npm](https://www.npmjs.com/package/@zusammen/nservicebus)   |
| [`@zusammen/express`](packages/express)         | Express 5 adapter                                                                      | [npm](https://www.npmjs.com/package/@zusammen/express)       |
| [`@zusammen/fastify`](packages/fastify)         | Fastify 5 adapter                                                                      | [npm](https://www.npmjs.com/package/@zusammen/fastify)       |
| [`@zusammen/nestjs`](packages/nestjs)           | NestJS 12 adapter                                                                      | [npm](https://www.npmjs.com/package/@zusammen/nestjs)        |
| [`@zusammen/hono`](packages/hono)               | Hono 4 adapter                                                                         | [npm](https://www.npmjs.com/package/@zusammen/hono)          |
| [`Zusammen.NServiceBus`](dotnet)                | For NServiceBus endpoints: resolves messages sent without a .NET type mapping          | [NuGet](https://www.nuget.org/packages/Zusammen.NServiceBus) |

### NServiceBus interoperability

Messages use a minimal, library-agnostic format by default. Opting into the NServiceBus convention and a NServiceBus routing topology makes them consumable by [NServiceBus](https://particular.net/nservicebus) endpoints using the RabbitMQ transport, following the [native integration](https://docs.particular.net/transports/rabbitmq/native-integration) guidance:

```typescript
import { nserviceBusConvention } from '@zusammen/nservicebus';
import { RabbitMQTransport } from '@zusammen/rabbitmq';
import { nserviceBusConventionalTopology } from '@zusammen/rabbitmq/nservicebus';

const factory = createSessionFactory({
  persistence,
  transport: new RabbitMQTransport({ url, topology: nserviceBusConventionalTopology() }),
  convention: nserviceBusConvention({
    endpointName: 'Sales.Api',
    messageTypes: new Map([[PlaceOrder, 'Sales.Messages.PlaceOrder']]), // .NET FullNames
    topics: { OrderPlaced: 'Sales.Messages:OrderPlaced' }, // where events are published
  }),
});
```

Messages without a .NET type mapping can be consumed by endpoints that reference the `Zusammen.NServiceBus` NuGet package (in [`dotnet/`](dotnet)) and call `endpointConfiguration.EnableZusammen()`, which matches them to the endpoint's message types by name. Compatibility is tested against real NServiceBus endpoints, and behavior is checked against the [NServiceBus TransactionalSession](https://github.com/Particular/NServiceBus.TransactionalSession) acceptance tests via a [conformance matrix](docs/nservicebus-conformance.md).

## Documentation

- Guides: [delivery guarantees](docs/guides/delivery-guarantees.md) (what receivers need to do), [message types](docs/guides/message-types.md) (naming, bundlers, NServiceBus mapping and topics), [operations](docs/guides/operations.md) (resources, control queues, timing, logging), [releasing](docs/guides/releasing.md) (for maintainers).
- Package READMEs: [core](packages/core), [mongodb](packages/mongodb), [rabbitmq](packages/rabbitmq), [nservicebus](packages/nservicebus), [express](packages/express), [fastify](packages/fastify), [nestjs](packages/nestjs), [hono](packages/hono), [Zusammen.NServiceBus](dotnet).
- Samples: [Node.js → Node.js](samples/node-to-node) (Express API and an idempotent worker), [Node.js → NServiceBus](samples/node-to-nservicebus) (Express API and a .NET NServiceBus endpoint).
- Design: [implementation plan](docs/node-js-transactional-session-implementation.md), [NServiceBus conformance matrix](docs/nservicebus-conformance.md).

## Development

Requirements: Node.js 22.13+, [pnpm](https://pnpm.io), Docker (for local services and integration tests), .NET 10 SDK (for the NServiceBus package and compatibility tests; the compatibility tests are skipped without it).

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

Releases are published by pushing a SemVer tag on `main`: see [releasing](docs/guides/releasing.md).

## License

[MIT](LICENSE)
