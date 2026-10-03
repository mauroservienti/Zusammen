# @zusammen/mongodb

MongoDB persistence for [Zusammen](https://github.com/mauroservienti/Zusammen) transactional sessions.

## Install

```sh
npm install @zusammen/core @zusammen/mongodb mongodb
```

Until 1.0, releases are prereleases on the `next` dist-tag: append `@next` to the `@zusammen/*` packages to get the latest one.

Requires a replica set or sharded cluster (MongoDB transactions) and the `mongodb` driver 7.

## Usage

```typescript
import { MongoClient } from 'mongodb';
import { createSessionFactory } from '@zusammen/core';
import { MongoDBPersistence } from '@zusammen/mongodb';

const client = new MongoClient('mongodb://localhost:27017/sales?replicaSet=rs0');
const persistence = new MongoDBPersistence({ client });
const factory = createSessionFactory({ persistence, transport });
await factory.start();

await using session = await factory.open();
// The transaction context is a ClientSession of the same client: pass it to your operations
await client.db().collection('orders').insertOne(order, { session: session.transactionContext });
await session.publish(new OrderPlaced(order.id));
await session.commit();
```

Use **the same `MongoClient`** for your data and for the persistence: sessions can't span clients.

## Options

| Option                    | Default                                  | Description                                               |
| ------------------------- | ---------------------------------------- | --------------------------------------------------------- |
| `client`                  |                                          | The application's `MongoClient`                           |
| `databaseName`            | the connection string's database         |                                                           |
| `collectionName`          | `zusammen_outbox`                        |                                                           |
| `retentionMs`             | 7 days                                   | How long dispatched records and tombstones are kept (TTL) |
| `transactionOptions`      | snapshot reads, majority writes, primary | Options for session transactions                          |
| `closeClientOnDisconnect` | `false`                                  | The client is usually shared with the application         |
| `logger`                  | silent                                   |                                                           |

## Resources

`createResources()` creates the outbox collection and a TTL index on `DispatchedAt` (and updates the TTL when `retentionMs` changes). `verifyResources()` fails if the collection is missing and warns if the TTL index is. Create the collections your sessions write to beforehand: MongoDB can't create the same collection implicitly from concurrent transactions.

## Behavior

- One outbox document per session (`_id` = session ID). Headers are stored as key/value pairs, since header names contain dots.
- Reads on the control path use the primary with majority read concern, so a committed session is never mistaken for a missing one.
- A late commit that conflicts with a tombstone (duplicate key or write conflict on the outbox insert) fails with `SessionCommitConflictError`.
- Commits are retried on `UnknownTransactionCommitResult`.
