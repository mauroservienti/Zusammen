# @zusammen/core

Transactional sessions for Node.js: business data and outgoing messages, committed together. Part of [Zusammen](https://github.com/mauroservienti/Zusammen).

## Install

```sh
npm install @zusammen/core
```

Until 1.0, releases are prereleases on the `next` dist-tag: append `@next` to the `@zusammen/*` packages to get the latest one.

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

### In a transaction the application runs

When an ORM or query builder runs the transaction through a callback (e.g. `prisma.$transaction(fn)`), let it: pass that API to `runInTransaction`, and the session joins its transaction.

```typescript
const order = await factory.runInTransaction(
  (work) => prisma.$transaction(work), // calls work with the transaction, commits when it returns
  async (session) => {
    const order = await session.transactionContext.order.create({ data }); // the ORM's transaction
    await session.publish(new OrderPlaced(order.id));
    return order;
  },
);
```

- The outbox record is stored in the application's transaction once the work returns; messages are dispatched once that transaction commits.
- Throw from the work to roll back. The session's `commit()` and `rollback()` throw `ApplicationTransactionError`: the application's transaction API commits and rolls back.
- `getSession()` works inside the work, as with `withSession`.
- A transaction API that retries its callback gets a new session per attempt.
- The persistence provider must support the application's transaction as its `TContext` (see its README).

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
| `FactoryNotStartedError`      | `open()` or `runInTransaction()` before `start()`                                                     |
| `ApplicationTransactionError` | `commit()` or `rollback()` on a session in `runInTransaction`                                         |
| `MissingResourcesError`       | Resources are missing at startup; `resources` lists them                                              |
| `UnknownMessageTypeError`     | The convention can't name a message                                                                   |
| `UnknownPublishTopicError`    | The convention needs a publish topic and has none                                                     |
| `UnroutableMessageError`      | The broker couldn't route a send (retried by the control message)                                     |
| `IncompatibleConventionError` | The transport's topology can't work with the convention                                               |

## Writing providers

Implement `PersistenceProvider<TContext>` or `TransportProvider`. `begin`, `commit` and `rollback` are optional: without them, the provider supports `runInTransaction` but not `open()`. Persistence providers must pass the shared contract suite (`persistenceProviderContract` in the repository's private `@zusammen/testing` package). See the [implementation plan](../../docs/node-js-transactional-session-implementation.md) for the semantics.
