# @zusammen/fastify

Fastify 5 integration for [Zusammen](https://github.com/mauroservienti/Zusammen) transactional sessions.

```typescript
import { sessionOf, zusammen } from '@zusammen/fastify';

await app.register(zusammen, { factory });

app.post('/orders', { config: { transactionalSession: true } }, async (request, reply) => {
  const session = sessionOf(request); // or getSession() anywhere in the call stack
  await orders.insertOne(request.body, { session: session.transactionContext });
  await session.publish(new OrderPlaced(request.body.id));
  return reply.code(201).send({ id: request.body.id });
});
```

Routes opt in with `config: { transactionalSession: true }`, or register with `global: true` for every route. The session is settled in `onSend`, before the response is written: status below 400 commits, otherwise it rolls back. A failed commit becomes an error response through Fastify's error handling. Aborted requests roll back.

| Option         | Default                  | Description                               |
| -------------- | ------------------------ | ----------------------------------------- |
| `factory`      |                          | A started session factory                 |
| `global`       | `false`                  | Open a session for every route            |
| `session`      |                          | Options for every opened session          |
| `shouldCommit` | `reply.statusCode < 400` | Decides commit or rollback before sending |
