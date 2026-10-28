# @zusammen/postgresql

PostgreSQL persistence for [Zusammen](https://github.com/mauroservienti/Zusammen) transactional sessions.

## Install

```sh
npm install @zusammen/core @zusammen/postgresql pg
```

Until 1.0, releases are prereleases on the `next` dist-tag: append `@next` to the `@zusammen/*` packages to get the latest one.

Requires the [`pg`](https://node-postgres.com) driver 8.11+. Tested with PostgreSQL 18.

## Usage

```typescript
import pg from 'pg';
import { createSessionFactory } from '@zusammen/core';
import { PostgreSQLPersistence } from '@zusammen/postgresql';

const pool = new pg.Pool({ connectionString: 'postgres://localhost:5432/sales' });
const persistence = new PostgreSQLPersistence({ pool });
const factory = createSessionFactory({ persistence, transport });
await factory.start();

await using session = await factory.open();
// The transaction context is a PoolClient inside a transaction: run your queries on it
await session.transactionContext.query('INSERT INTO orders (id) VALUES ($1)', [order.id]);
await session.publish(new OrderPlaced(order.id));
await session.commit();
```

Queries that run on the pool, or on another client, are **not** part of the session's transaction.

### ORMs and query builders

Libraries that accept an existing `pg` client can run on the session's client. With [Drizzle](https://orm.drizzle.team):

```typescript
import { drizzle } from 'drizzle-orm/node-postgres';

const db = drizzle(session.transactionContext);
await db.insert(orders).values({ id: order.id });
```

### In a transaction the application runs

With `factory.runInTransaction`, the application's transaction API runs the transaction and the session joins it. When you run your own `pg` transactions, `PostgreSQLPersistence` works as is: the context is your `PoolClient`.

ORMs that manage their own connections and transactions (e.g. Prisma, TypeORM, Sequelize) hand out their own transaction objects. Use `PostgreSQLOrmPersistence` with an adapter that runs a statement in that transaction; only the outbox insert runs in it, the rest of the outbox's queries use the pool. Adapters for Prisma, Drizzle, TypeORM, Sequelize, Knex and Kysely are included; for another library, implement `ApplicationTransactions`:

```typescript
import { PostgreSQLOrmPersistence, type ApplicationTransactions } from '@zusammen/postgresql';

const transactions: ApplicationTransactions<OrmTransaction> = {
  // The statement has `$1`-style parameters
  execute: (tx, sql, parameters) => tx.rawQuery(sql, parameters),
};
const persistence = new PostgreSQLOrmPersistence({ pool, transactions });
```

`factory.open()` isn't available with `PostgreSQLOrmPersistence`: the ORM owns the transactions. Unique violations are found in the error the ORM wraps the driver's error in (`cause`, TypeORM's `driverError`, Sequelize's `parent`), or with the adapter's `sqlState(error)` for ORMs that don't keep it, so a session that finds a tombstone still fails with `SessionCommitConflictError`. The pool must reach the same database as the ORM. See the [core README](../core/README.md#in-a-transaction-the-application-runs) for the behavior.

#### Prisma

`@zusammen/postgresql/prisma` provides the adapter, for Prisma 7 interactive transactions (tested with `@prisma/adapter-pg`, which can share the pool). `prisma` only gives the transaction type:

```typescript
import { PrismaPg } from '@prisma/adapter-pg';
import { prismaTransactions } from '@zusammen/postgresql/prisma';

const prisma = new PrismaClient({ adapter: new PrismaPg(pool) });
const persistence = new PostgreSQLOrmPersistence({ pool, transactions: prismaTransactions(prisma) });
// ...

await factory.runInTransaction(
  (work) => prisma.$transaction(work),
  async (session) => {
    await session.transactionContext.order.create({ data: order });
    await session.publish(new OrderPlaced(order.id));
  },
);
```

#### Drizzle

`@zusammen/postgresql/drizzle` provides the adapter, for any Drizzle PostgreSQL driver (requires `drizzle-orm` 0.45+). `db` only gives the transaction type:

```typescript
import { drizzle } from 'drizzle-orm/node-postgres';
import { drizzleTransactions } from '@zusammen/postgresql/drizzle';

const db = drizzle(pool);
const persistence = new PostgreSQLOrmPersistence({ pool, transactions: drizzleTransactions(db) });
// ...

await factory.runInTransaction(
  (work) => db.transaction(work),
  async (session) => {
    await session.transactionContext.insert(orders).values({ id: order.id });
    await session.publish(new OrderPlaced(order.id));
  },
);
```

#### TypeORM

`@zusammen/postgresql/typeorm` provides the adapter (tested with TypeORM 1.1). `dataSource` only gives the entity manager type:

```typescript
import { typeormTransactions } from '@zusammen/postgresql/typeorm';

const persistence = new PostgreSQLOrmPersistence({ pool, transactions: typeormTransactions(dataSource) });
// ...

await factory.runInTransaction(
  (work) => dataSource.transaction(work),
  async (session) => {
    await session.transactionContext.insert(Order, { id: order.id });
    await session.publish(new OrderPlaced(order.id));
  },
);
```

#### Sequelize

`@zusammen/postgresql/sequelize` provides the adapter, for managed transactions (requires `sequelize` 6.37+). The transaction context is the Sequelize `Transaction`: pass it to your queries.

```typescript
import { sequelizeTransactions } from '@zusammen/postgresql/sequelize';

const persistence = new PostgreSQLOrmPersistence({ pool, transactions: sequelizeTransactions(sequelize) });
// ...

await factory.runInTransaction(
  (work) => sequelize.transaction(work),
  async (session) => {
    await Order.create({ id: order.id }, { transaction: session.transactionContext });
    await session.publish(new OrderPlaced(order.id));
  },
);
```

#### Knex

`@zusammen/postgresql/knex` provides the adapter (requires `knex` 3). The transaction context is the Knex transaction:

```typescript
import { knexTransactions } from '@zusammen/postgresql/knex';

const persistence = new PostgreSQLOrmPersistence({ pool, transactions: knexTransactions() });
// ...

await factory.runInTransaction(
  (work) => knex.transaction(work),
  async (session) => {
    await session.transactionContext('orders').insert({ id: order.id });
    await session.publish(new OrderPlaced(order.id));
  },
);
```

#### Kysely

`@zusammen/postgresql/kysely` provides the adapter (requires `kysely` 0.28+). `db` only gives the database type:

```typescript
import { kyselyTransactions } from '@zusammen/postgresql/kysely';

const persistence = new PostgreSQLOrmPersistence({ pool, transactions: kyselyTransactions(db) });
// ...

await factory.runInTransaction(
  (work) => db.transaction().execute(work),
  async (session) => {
    await session.transactionContext.insertInto('orders').values({ id: order.id }).execute();
    await session.publish(new OrderPlaced(order.id));
  },
);
```

## Options

| Option                  | Default            | Description                                                                         |
| ----------------------- | ------------------ | ----------------------------------------------------------------------------------- |
| `pool`                  |                    | The application's `pg.Pool`                                                         |
| `transactions`          |                    | `PostgreSQLOrmPersistence` only: runs the outbox insert                             |
| `schemaName`            | `public`           | Must exist                                                                          |
| `tableName`             | `zusammen_outbox`  |                                                                                     |
| `retentionMs`           | 7 days             | How long dispatched records and tombstones are kept                                 |
| `cleanupIntervalMs`     | 1 hour             | How often expired records are deleted while connected; 0: never                     |
| `isolationLevel`        | the server default | `PostgreSQLPersistence` only: `read committed`, `repeatable read` or `serializable` |
| `closePoolOnDisconnect` | `false`            | The pool is usually shared with the application                                     |
| `logger`                | silent             |                                                                                     |

## Resources

`createResources()` creates the outbox table and a partial index on `dispatched_at` for cleanup; concurrent calls from several instances are serialized with an advisory lock. `verifyResources()` fails if the table is missing and warns if the index is.

```sql
CREATE TABLE zusammen_outbox (
  id text PRIMARY KEY,
  dispatched boolean NOT NULL DEFAULT false,
  dispatched_at timestamptz,
  transport_operations jsonb NOT NULL
);
CREATE INDEX zusammen_outbox_dispatched_at ON zusammen_outbox (dispatched_at) WHERE dispatched;
```

## Cleanup

PostgreSQL has no TTL: every connected instance deletes dispatched records and tombstones older than `retentionMs` every `cleanupIntervalMs`, in batches that skip rows locked by another instance. To run it from a scheduled job instead, set `cleanupIntervalMs: 0` and call `persistence.removeExpired()`, or run:

```sql
DELETE FROM zusammen_outbox WHERE dispatched AND dispatched_at < now() - interval '7 days';
```

Records that were never dispatched are kept: the control message is still responsible for them.

## Behavior

- One outbox row per session (`id` = session ID). Transport operations are stored as JSON, with base64 bodies, and cleared once the record is dispatched.
- A tombstone stored while a session's transaction has already inserted its row waits for that transaction: it reports `exists` if the transaction commits and is stored if it rolls back. A session whose outbox insert finds a tombstone fails with `SessionCommitConflictError` (unique violation, or a serialization failure under `serializable`).
- Control path reads use the pool. Point it at the primary: a read replica can lag, and a committed session would be mistaken for a missing one.
- Committing a transaction that an earlier error aborted fails, rather than reporting success for what PostgreSQL silently rolled back.
- `commit()` and `rollback()` release the client to the pool; a client whose connection may be broken is discarded.
