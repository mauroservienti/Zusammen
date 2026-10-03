# @zusammen/hono

Hono 4 integration for [Zusammen](https://github.com/mauroservienti/Zusammen) transactional sessions, for long-running Node.js, Bun or Deno servers (Zusammen needs a process consuming the control queue).

## Install

```sh
npm install @zusammen/core @zusammen/hono hono
```

Until 1.0, releases are prereleases on the `next` dist-tag: append `@next` to the `@zusammen/*` packages to get the latest one.

```typescript
import { sessionOf, transactionalSession, type TransactionalSessionVariables } from '@zusammen/hono';

const app = new Hono<{ Variables: TransactionalSessionVariables }>();

app.post('/orders', transactionalSession(factory), async (c) => {
  const session = sessionOf(c); // or getSession() anywhere in the call stack
  const order = await c.req.json();
  await orders.insertOne(order, { session: session.transactionContext });
  await session.publish(new OrderPlaced(order.id));
  return c.json({ id: order.id }, 201);
});
```

The session is settled after the handler and before the response is returned: status below 400 commits, otherwise (or when the handler throws) it rolls back. If the commit fails, the response is replaced, headers included, by an empty 500 or `onCommitError`'s response.

| Option          | Default              | Description                        |
| --------------- | -------------------- | ---------------------------------- |
| `session`       |                      | Options for every opened session   |
| `shouldCommit`  | `c.res.status < 400` | Decides commit or rollback         |
| `onCommitError` | empty 500            | The response when the commit fails |
