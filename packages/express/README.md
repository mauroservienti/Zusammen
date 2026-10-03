# @zusammen/express

Express 5 integration for [Zusammen](https://github.com/mauroservienti/Zusammen) transactional sessions.

## Install

```sh
npm install @zusammen/core @zusammen/express express
```

Until 1.0, releases are prereleases on the `next` dist-tag: append `@next` to the `@zusammen/*` packages to get the latest one.

```typescript
import { getSession } from '@zusammen/core';
import { sessionOf, transactionalSession } from '@zusammen/express';

app.post('/orders', transactionalSession(factory), async (req, res) => {
  const session = sessionOf(req); // or getSession() anywhere in the call stack
  await orders.insertOne(req.body, { session: session.transactionContext });
  await session.publish(new OrderPlaced(req.body.id));
  res.status(201).json({ id: req.body.id });
});
```

The middleware opens a session per request and settles it **before the response reaches the client**: it holds the response when the handler starts sending it, commits (status below 400) or rolls back (400 and above, thrown errors), then lets the response through. If the commit fails, the client gets an empty 500 instead, without the handler's headers. Aborted requests roll back.

| Option          | Default                | Description                                         |
| --------------- | ---------------------- | --------------------------------------------------- |
| `session`       |                        | Options for every opened session                    |
| `shouldCommit`  | `res.statusCode < 400` | Decides commit or rollback when the response starts |
| `onCommitError` | empty 500              | Responds when the commit fails                      |

Handlers can commit or roll back themselves; the middleware then leaves the session alone. Streamed responses are delivered after the commit, buffered in memory until then.
