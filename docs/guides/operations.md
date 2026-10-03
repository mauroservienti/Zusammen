# Operations

## Resources

Zusammen doesn't create collections, indexes, queues or exchanges unless asked to. At startup, `factory.start()` checks that everything exists and fails with `MissingResourcesError`, listing everything missing.

Provision resources in one of two ways:

- **At deployment** (recommended for production): run a script that connects the providers and calls `createResources()`:

  ```typescript
  await persistence.connect();
  await persistence.createResources();
  await transport.connect();
  await transport.createResources();
  await transport.disconnect();
  ```

- **At startup**: `createSessionFactory({ ..., createResources: true })`.

| Provider | Resources                                                                                                                                                                                                                         |
| -------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| MongoDB  | Outbox collection (default `zusammen_outbox`) and its TTL index on `DispatchedAt` (default retention 7 days). A missing TTL index is only a warning.                                                                              |
| RabbitMQ | Control queue (default `zusammen.control`, quorum), error queue (`<control>.error`), delay queues `<control>.delay.{1,2,4,8,16,32,64}s` (classic, TTL), and the topology's exchanges (`zusammen.events` for the default topology) |

**Business collections.** MongoDB can't create the same collection implicitly from concurrent transactions: create the collections your sessions write to beforehand.

**NServiceBus conventional topology.** Event exchanges (`Namespace:TypeName`) are created by NServiceBus subscribers. Until one exists, publishing that event is skipped (nobody is subscribed). With `createResources: true`, Zusammen declares them on first publish, like NServiceBus publishers do.

## Control messages

Every application instance that calls `factory.start()` consumes the control queue, competing with the other instances. Any instance can process any control message; all it needs is access to the broker and the database. An instance may stop or crash at any time after a commit: the control message is handled by another instance, or by the next one to start.

- **At least one instance must be running** for messages whose immediate dispatch failed to be delivered.
- **One control queue per application.** Applications sharing a broker must use different `controlQueue` names; instances of the same application share theirs.
- **Error queue.** Control messages that keep failing (e.g. the broker rejects dispatches for a missing destination) end up in `<control>.error` after 10 failures, with the reason in the `zusammen.error` header. Monitor it; move messages back to the control queue to retry once the cause is fixed.
- **Serverless** deployments with RabbitMQ are not supported: nothing would consume the control queue.

## Timing

| Setting                                       | Default | Meaning                                                                |
| --------------------------------------------- | ------- | ---------------------------------------------------------------------- |
| `maxCommitDurationMs` (factory, per session)  | 15 s    | How long a session may take from `commit()` to the database commit     |
| `controlTiming.initialCommitDelayIncrementMs` | 1 s     | Initial control message delay, and the first wait for a pending commit |
| `controlTiming.maxCommitDelayIncrementMs`     | 10 s    | Cap for the wait between checks for a pending commit                   |
| `controlTiming.maxFailures`                   | 10      | Failures before a control message moves to the error queue             |
| `controlTiming.initialFailureDelayMs`         | 1 s     | First delay after a failure, doubling                                  |
| `controlTiming.maxFailureDelayMs`             | 60 s    | Cap for the delay after a failure                                      |

RabbitMQ delays are rounded up to the delay queue levels (1, 2, 4 … 64 s).

## Logging

Pass a `logger` (`debug`, `info`, `warn`, `error`, each with a message and structured context) to `createSessionFactory`, `RabbitMQTransport` and `MongoDBPersistence`. Worth alerting on: _"Giving up on control message"_ (error queue), _"Commit window expired … stored a tombstone"_ (a session didn't commit in time), and RabbitMQ reconnection failures.
