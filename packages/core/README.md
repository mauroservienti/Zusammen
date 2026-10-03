# @zusammen/core

Transactional sessions for Node.js: business data and outgoing messages, committed together. Part of [Zusammen](https://github.com/mauroservienti/Zusammen).

Pair it with a persistence (e.g. [`@zusammen/mongodb`](../mongodb)) and a transport (e.g. [`@zusammen/rabbitmq`](../rabbitmq)).

## Usage

```typescript
import { createSessionFactory } from '@zusammen/core';

const factory = createSessionFactory({ persistence, transport });
await factory.start(); // verifies resources, starts processing control messages

await using session = await factory.open();
await orders.insertOne(order, { session: session.transactionContext });
await session.publish(new OrderPlaced(order.id));
await session.send('billing', new ChargeCustomer(order.id));
await session.commit();
```

`await using` rolls the session back if it isn't committed. Sessions are single use.

### Without passing the session around

```typescript
import { getSession, withSession } from '@zusammen/core';

await withSession(factory, async () => {
  await placeOrder(order); // calls getSession() internally
}); // commits on success, rolls back on throw
```

For web frameworks, use the adapters: [Express](../express), [Fastify](../fastify), [NestJS](../nestjs), [Hono](../hono).

## Factory options

| Option                | Default                                                  | Description                                                                   |
| --------------------- | -------------------------------------------------------- | ----------------------------------------------------------------------------- |
| `persistence`         |                                                          | A `PersistenceProvider`                                                       |
| `transport`           |                                                          | A `TransportProvider`                                                         |
| `convention`          | `zusammenConvention()`                                   | Wire format of outgoing messages                                              |
| `createResources`     | `false`                                                  | Create collections, queues, … at startup instead of only verifying they exist |
| `maxCommitDurationMs` | `15_000`                                                 | Commit window; also per session via `factory.open({ maxCommitDurationMs })`   |
| `controlTiming`       | see [operations](../../docs/guides/operations.md#timing) | Delays and limits for control message processing                              |
| `logger`              | silent                                                   | `{ debug, info, warn, error }`                                                |

## Default convention

`zusammenConvention({ messageTypes, topics, serializer })` sends JSON with the headers `zusammen.message-id`, `zusammen.message-type`, `zusammen.intent`, `zusammen.time-sent` (ISO 8601) and `zusammen.session-id`. See [message types](../../docs/guides/message-types.md).

## Errors

| Error                         | When                                                                                                  |
| ----------------------------- | ----------------------------------------------------------------------------------------------------- |
| `SessionCommitConflictError`  | The commit took longer than the commit window and the safety net gave up on it; nothing was committed |
| `SessionClosedError`          | Using a session after commit or rollback                                                              |
| `FactoryNotStartedError`      | `open()` before `start()`                                                                             |
| `MissingResourcesError`       | Resources are missing at startup; `resources` lists them                                              |
| `UnknownMessageTypeError`     | The convention can't name a message                                                                   |
| `UnknownPublishTopicError`    | The convention needs a publish topic and has none                                                     |
| `UnroutableMessageError`      | The broker couldn't route a send (retried by the control message)                                     |
| `IncompatibleConventionError` | The transport's topology can't work with the convention                                               |

## Writing providers

Implement `PersistenceProvider<TContext>` or `TransportProvider`. Persistence providers must pass the shared contract suite (`persistenceProviderContract` in the repository's private `@zusammen/testing` package). See the [implementation plan](../../docs/node-js-transactional-session-implementation.md) for the semantics.
