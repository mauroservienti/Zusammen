# Zusammen — Node.js Transactional Session Implementation Plan

## Overview

Zusammen is a transactional session library for Node.js. It atomically couples business data changes with outgoing messages using the Outbox pattern. Dispatch is guaranteed by a **control message** sent through the transport, instead of a background poller. Initial support targets RabbitMQ (transport) and MongoDB (persistence), behind provider interfaces so others can be added.

### Delivery guarantees

- **Atomic state change**: business data and outgoing messages are committed together or not at all.
- **At-least-once dispatch**: every committed message is eventually dispatched, possibly more than once (e.g., crash between dispatch and marking dispatched, or immediate dispatch racing the control message).
- **Stable message IDs**: each outgoing message gets an ID at `send`/`publish` time that survives re-dispatch, so receivers can deduplicate.

Zusammen does **not** provide exactly-once delivery; receivers must be idempotent (or use an inbox/outbox on their side).

## Control Message Approach

If the session has no outgoing operations, `commit()` only commits the database transaction: no control message, no outbox record. Otherwise, on `commit()`:

1. Send the control message (with publisher confirms), delayed by the initial commit delay increment (default 1 s) so that it normally arrives after the immediate dispatch completed instead of racing it and causing a duplicate dispatch. The delay doesn't count against the commit window. If sending fails, roll back and throw.
2. Store the outbox record (one document per session, `_id = sessionId`) inside the database transaction.
3. Commit the database transaction.
4. Best-effort immediate dispatch of the outbox operations, then mark the record as dispatched. Failures here are logged, not thrown — the control message covers them.

When the control message is consumed:

| Outbox record state               | Within commit window              | Window expired                 |
| --------------------------------- | --------------------------------- | ------------------------------ |
| Exists, dispatched (or tombstone) | ack (no-op)                       | ack (no-op)                    |
| Exists, not dispatched            | dispatch, mark dispatched, ack    | dispatch, mark dispatched, ack |
| Does not exist                    | retry with delay (commit pending) | store **tombstone**, ack       |

### Tombstone

A tombstone is an outbox record with `_id = sessionId`, already marked dispatched and with no operations. If the original transaction later tries to commit, its outbox insert fails with a duplicate key error, so the session commit fails and the user sees an error. This guarantees that a commit can never succeed without a safety net, and that the control message never loops forever for a rolled-back or crashed session.

If the tombstone insert itself hits a duplicate key (the commit landed concurrently), re-read the record and handle it as "exists".

### Failure scenarios

- **Normal flow**: immediate dispatch succeeds; the control message finds the record dispatched → no-op.
- **Crash or rollback before commit**: control message retries until the window expires, then stores a tombstone → no-op.
- **Crash after commit, before dispatch**: control message dispatches.
- **Immediate dispatch fails**: control message dispatches.
- **Control message arrives before commit**: delayed retry until the record appears.
- **Commit slower than the window**: tombstone wins; commit fails with a duplicate key → user gets an error, nothing is dispatched, no business data committed.
- **Multiple instances**: competing consumers on the control queue; each control message is processed by one instance at a time.
- **Poison control message** (e.g., dispatch keeps failing): after N attempts it moves to an error queue.

## Architecture

pnpm monorepo. Core ships a neutral wire format; NServiceBus compatibility is opt-in.

- `@zusammen/core` — `TransactionalSession`, session factory, `ControlMessageHandler` (transport-agnostic decision logic), `withSession` + `AsyncLocalStorage` session context, provider contracts, `MessageConvention` contract + the default Zusammen convention, errors. No runtime dependencies.
- `@zusammen/mongodb` — `MongoDBPersistence` (transaction context: the MongoDB `ClientSession`)
- `@zusammen/rabbitmq` — `RabbitMQTransportProvider`, `RoutingTopology` contract + the default Zusammen topology
  - `@zusammen/rabbitmq/nservicebus` (subpath export) — NServiceBus conventional and direct routing topologies
- `@zusammen/nservicebus` — transport-agnostic NServiceBus message convention (headers, type names, serialization). Opt-in.
- `@zusammen/express`, `@zusammen/fastify`, `@zusammen/nestjs`, `@zusammen/hono` — thin web framework adapters (see Integrations).

Future transports follow the same split, e.g. `@zusammen/sqs` + `@zusammen/sqs/nservicebus`, where the SQS-specific NServiceBus rules are much lighter than RabbitMQ's.

### Extension points

Two independent seams decide what goes on the wire:

1. **`MessageConvention`** (core, transport-agnostic) — runs at `send`/`publish` time and turns a user message into a `TransportOperation`: resolves the message type name, builds headers, serializes the body, fills generic native properties (content type, correlation ID, reply-to).
2. **`RoutingTopology`** (per transport) — runs at dispatch time and decides where an operation goes (exchange, routing key, `mandatory`) and which exchanges to declare.

### Control message processing: decision vs. delivery

- **Core decides.** `ControlMessageHandler` takes a control message plus the outbox state and returns a `ControlResult` (ack, retry with delay, error), storing the tombstone or dispatching through the providers as needed. It knows nothing about how the message arrived and is the main unit-test target.
- **The transport delivers.** Each transport owns how control messages arrive and how a `ControlResult` is applied (`consumeControl`). For RabbitMQ that means monitoring the control queue: prefetch, ack, republish to TTL retry queues, dead-letter to the error queue, reconnects, graceful shutdown. Retries are delayed by the broker (TTL retry queues), never by a consumer staying alive.
- Any consumer can process any control message; all it needs is access to the broker and the outbox storage. The process that opened the session may die right after commit.
- Transports where the platform delivers messages (e.g. a future SQS transport with Lambda triggers) can expose a single-message entry point inside that transport; core does not change.

Because operations are stored in the outbox **after** the convention has run, re-dispatch by the control message produces byte-identical messages, and changing convention config never affects already-committed messages.

## Core Interfaces

The contracts live in `packages/core/src` and are the source of truth; this is a summary.

| File             | Contents                                                                                                                                                                   |
| ---------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `outbox.ts`      | `OutboxRecord` (one per session, `id` = session ID), `TransportOperation` (prepared message: ID, intent, destination/topic, type, headers, binary body, native properties) |
| `control.ts`     | `ControlMessage` (session ID, remaining commit duration, delay increment, attempt), `ControlResult` (`ack` / `retry` with next message / `error`), `ControlMessageHandler` |
| `convention.ts`  | `MessageConvention` (`name`, `toOperation`), `OutgoingMessage`, `ConventionContext` (session ID, injectable clock and ID generator), `SendOptions`, `PublishOptions`       |
| `persistence.ts` | `PersistenceProvider<TContext>`: connect/disconnect, begin/storeOutbox/commit/rollback, get/markDispatched/storeTombstone                                                  |
| `transport.ts`   | `TransportProvider`: connect/disconnect, dispatch, sendControl, consumeControl (returns a stop function), optional validateConvention                                      |
| `session.ts`     | `TransactionalSession<TContext>`, `SessionFactory<TContext>`, `SessionFactoryOptions`, `OpenSessionOptions`, `DEFAULT_MAX_COMMIT_DURATION_MS`                              |
| `errors.ts`      | `ZusammenError` and subclasses (below)                                                                                                                                     |
| `logger.ts`      | `Logger` (debug/info/warn/error with structured context), `silentLogger` default                                                                                           |

Errors: `SessionCommitConflictError` (outbox record already exists, typically a tombstone after the commit window expired), `SessionClosedError`, `FactoryNotStartedError`, `UnknownMessageTypeError`, `UnknownPublishTopicError`, `UnroutableMessageError` (unroutable send), `IncompatibleConventionError` (topology rejects the convention).

Notes:

- `ControlMessage.remainingCommitDurationMs` is decremented by each retry delay rather than compared against a timestamp, so processing never depends on clocks agreeing across machines.
- Message type and topic registries are options of the convention (e.g. `zusammenConvention({ messageTypes, topics })`), not of the factory.
- The transport maps `ControlResult` to native actions; no delivery tags leak into core.

### User-facing API

```typescript
interface TransactionalSession<TContext> extends AsyncDisposable {
  readonly sessionId: string;
  readonly transactionContext: TContext; // e.g., MongoDB ClientSession to pass to user operations
  send(destination: string, message: unknown, options?: SendOptions): Promise<void>; // convention runs now, operation buffered in memory
  publish(message: unknown, options?: PublishOptions): Promise<void>; // convention runs now, operation buffered in memory
  commit(): Promise<void>;
  rollback(): Promise<void>;
  [Symbol.asyncDispose](): Promise<void>; // rolls back if neither committed nor rolled back
}

// Default: Zusammen convention + Zusammen topology
const factory = createSessionFactory({ persistence, transport, maxCommitDurationMs: 15_000 });
await factory.start(); // connects providers and starts control message processing
await using session = await factory.open({ maxCommitDurationMs: 5_000 }); // throws if the factory is not started
await orders.insertOne(order, { session: session.transactionContext });
await session.publish(new OrderPlaced(order.id));
await session.commit();
```

Opting into NServiceBus compatibility:

```typescript
import { nserviceBusConvention } from '@zusammen/nservicebus';
import { RabbitMQTransportProvider } from '@zusammen/rabbitmq';
import { nserviceBusConventionalTopology } from '@zusammen/rabbitmq/nservicebus';

const transport = new RabbitMQTransportProvider({ url, topology: nserviceBusConventionalTopology() });
const factory = createSessionFactory({
  persistence,
  transport,
  convention: nserviceBusConvention({
    endpointName: 'Sales.Api',
    messageTypes: { OrderPlaced: 'Sales.Messages.OrderPlaced', PlaceOrder: 'Sales.Messages.PlaceOrder' },
  }),
});
```

The factory calls `transport.validateConvention(convention)` at startup; the NServiceBus RabbitMQ topologies throw if the convention is not `nservicebus`, so a half-configured setup fails fast.

## Key Design Decisions

1. **One outbox document per session**, `_id = sessionId` — enables the tombstone and atomic `markDispatched`.
2. **Commit window**: `maxCommitDurationMs` default 15 s, configurable per factory and per session.
3. **Retry delay**: while waiting for a commit, the delay starts at 1 s and doubles per attempt, capped at 10 s and at the remaining window, so the last retry lands exactly when the window expires. Failures (dispatch or storage errors) are counted separately in `ControlMessage.failures`, retried with their own backoff (1 s doubling, capped at 60 s), and never consume the commit window. All values are configurable via `controlTiming`.
4. **RabbitMQ delays without plugins**: one classic queue per delay level (1, 2, 4 … 64 s) with a queue-level TTL that dead-letters back to the control queue; requested delays round up to the next level (never shorter, so commits always get at least their full window). Per-message TTLs on a shared queue would only expire at the head. Control and error queues are quorum queues by default. Connection recovery uses amqplib's built-in recovery, reopening channels and resuming the consumer after every reconnect (resources are durable and never declared on reconnect).
5. **Error queue**: after `maxFailures` (default 10) the handler returns `error` and the transport moves the control message to `zusammen.control.error`.
6. **Cleanup**: TTL index on `dispatchedAt` (configurable retention, default 7 days).
7. **Wire format is pluggable**: `MessageConvention` + per-transport `RoutingTopology`; the default is a minimal Zusammen format, NServiceBus is opt-in.
8. **`mandatory` only for sends**: an unroutable send is a dispatch failure (retried by the control message); an event with no subscribers is legitimate and must not fail.
9. **TypeScript target**: ES2023, Node 22.13+, ESM-only builds via `tsc -b` (CommonJS consumers use `require(esm)`, stable on Node 22.12+). TypeScript 6.0 until typescript-eslint supports TypeScript 7.
10. **Every sender is a processor**: there is no separate processor role. `factory.start()` connects the providers and starts the transport's control message processing (for RabbitMQ: monitoring the control queue as one of the competing consumers), so any process that can open sessions also processes control messages. `open()` before `start()` throws. NServiceBus's separate `ProcessorEndpoint` exists for licensing reasons, not architectural ones.
11. **Control queue per factory**: the control queue name is configurable (default `zusammen.control`), so multiple factories in one process, or multiple applications on one broker, stay independent.
12. **Read-your-commit on the control path**: the control handler reads outbox records with consistency guarantees that see any committed transaction (MongoDB: primary read preference, `majority` read and write concern), so a committed record is never mistaken for a missing one.
13. **Serverless with RabbitMQ is deferred**: e.g. Amazon MQ + Lambda event-source mappings. Serverless support is expected to come naturally with an SQS transport, where Lambda triggers are the normal way to consume.
14. **Resources are user-owned by default**: providers never create collections, indexes, queues or exchanges unless `createSessionFactory({ createResources: true })` is set. By default `start()` verifies that everything exists and fails with `MissingResourcesError` listing all missing resources (a missing MongoDB TTL index is only logged). Providers expose `createResources()` / `verifyResources()` so deployment scripts can provision resources without starting a factory.

## Default Zusammen Wire Format

Deliberately minimal, so any consumer (Node, .NET, Python, …) can read it without special libraries.

### Convention

- Headers:
  - `zusammen.message-id` — UUID, also the native message ID (AMQP `message_id`)
  - `zusammen.message-type` — resolved type name
  - `zusammen.intent` — `send` or `publish`
  - `zusammen.time-sent` — ISO 8601 UTC
  - `zusammen.session-id` — originating transactional session
  - user-supplied headers merged last; cannot override `zusammen.*`
- Message type resolution: `options.messageType` → the convention's `messageTypes` registry → class name (`message.constructor.name`, unless it is `Object`) → `UnknownMessageTypeError` at `send`/`publish` time.
- Body: UTF-8 JSON, property names preserved as-is, `content_type: application/json`. Pluggable serializer.

### RabbitMQ topology

- Send: default exchange, routing key = destination queue name, `mandatory: true`.
- Publish: durable topic exchange `zusammen.events`, routing key = `topic` if supplied (option or the convention's `topics` registry), otherwise the message type. Subscribers bind their own queues.
- AMQP properties: `message_id`, `content_type`, `type` = message type, `delivery_mode: 2`, `correlation_id`/`reply_to` when set by the convention.

## NServiceBus Compatibility (opt-in)

Enabled via `@zusammen/nservicebus` + `@zusammen/rabbitmq/nservicebus`. When enabled, messages dispatched by Zusammen **must** be consumable by NServiceBus endpoints using the RabbitMQ transport, per the [native integration](https://docs.particular.net/transports/rabbitmq/native-integration) guidance. Internal formats (outbox document, control message) are never NServiceBus-specific.

### Convention (`@zusammen/nservicebus`, transport-agnostic)

Required headers:

- `NServiceBus.MessageId` — same value as the native message ID
- `NServiceBus.EnclosedMessageTypes` — .NET `FullName` of the message type (`Namespace.TypeName`) **when explicitly mapped**; NServiceBus maps FullName to any loaded type regardless of assembly. When not mapped, the header is omitted and the receiver derives it from `zusammen.message-type` (see below)
- `NServiceBus.ContentType` — `application/json`
- `NServiceBus.MessageIntent` — `Send` or `Publish`
- `NServiceBus.TimeSent` — UTC, format `yyyy-MM-dd HH:mm:ss:ffffff Z` (note the `:` before microseconds; JS only has ms precision, pad with zeros)

Set by default (recommended):

- `NServiceBus.ConversationId` — new UUID per session unless supplied
- `NServiceBus.CorrelationId` — defaults to the message ID unless supplied
- `NServiceBus.OriginatingEndpoint` — configured `endpointName`
- `NServiceBus.OriginatingMachine` — `os.hostname()`
- `NServiceBus.ReplyToAddress` — only if the user configures a reply queue

Not set: `NServiceBus.Version` (we are not NServiceBus); user-supplied headers are merged last and may not override `MessageId`/`MessageIntent`.

Message type mapping — JS has no .NET types. The NServiceBus convention **always** sets `zusammen.message-type` (resolved like the default convention: option → registry → class name) and additionally sets `NServiceBus.EnclosedMessageTypes` only when an explicit .NET FullName is available:

1. `options.messageType` on `send`/`publish`
2. The `messageTypes` registry, keyed by class name / string tag
3. Otherwise `EnclosedMessageTypes` is omitted

Contract: **no `EnclosedMessageTypes` header means "derive it from `zusammen.message-type`"**. The receiving NServiceBus endpoint must have the `Zusammen.NServiceBus` behavior installed (see below); without it, the message fails on the receiver and goes to its error queue. The value is never copied into `EnclosedMessageTypes`, so the behavior never has to guess whether a value is a real FullName.

The fallback applies to sends and publishes alike for type resolution; publish **routing** never depends on the type (see `topic` below).

Body serialization:

- UTF-8 JSON, no `$type` property (type comes from the header).
- NServiceBus's default `SystemJsonSerializer` (System.Text.Json) matches property names **case-sensitively**, so camelCase JS objects would not bind to PascalCase .NET properties. Default: serialize property names as **PascalCase**; configurable (`propertyNaming: 'pascal' | 'preserve'`) for receivers configured with camelCase options.
- Dates as ISO 8601 strings; `bigint` rejected unless a custom serializer is supplied.

### RabbitMQ topologies (`@zusammen/rabbitmq/nservicebus`)

AMQP properties:

Verified against NServiceBus.RabbitMQ 11.2.1 (`BasicPropertiesExtensions`, `MessageConverter`, `ConventionalRoutingTopology`, `DirectRoutingTopology`, `DefaultRoutingKeyConvention`) and NServiceBus 10 (`DateTimeOffsetHelper`, System.Text.Json serializer defaults).

AMQP properties, set like `BasicPropertiesExtensions.Fill`:

| Property         | Value                                                                   | Notes                                                                                                                                                      |
| ---------------- | ----------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `message_id`     | operation `messageId`                                                   | **Required**: the transport throws without it                                                                                                              |
| `content_type`   | `NServiceBus.ContentType`, else `application/octet-stream`              | Copied into `NServiceBus.ContentType` on receive                                                                                                           |
| `delivery_mode`  | `2` (persistent)                                                        |                                                                                                                                                            |
| `type`           | `NServiceBus.EnclosedMessageTypes` up to the first `,`, **only if set** | On receive, a missing `EnclosedMessageTypes` is filled from `type`: setting it for unmapped types would hide them from the `Zusammen.NServiceBus` behavior |
| `correlation_id` | `NServiceBus.CorrelationId`                                             | Copied back into the header on receive                                                                                                                     |
| `reply_to`       | `NServiceBus.ReplyToAddress`, if set                                    | Copied back into the header on receive                                                                                                                     |
| `headers`        | all operation headers, as strings                                       |                                                                                                                                                            |

Routing:

| Topology     | Send                                                 | Publish (`topic` = …)                                                                                                                                                                                 |
| ------------ | ---------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Conventional | fanout exchange named after the destination endpoint | fanout exchange named `Namespace:TypeName` (`Type.Namespace + ":" + Type.Name`), e.g. `Sales.Messages:OrderPlaced`                                                                                    |
| Direct       | default exchange, routing key = destination endpoint | routing key on `amq.topic`: the FullName with `.` replaced by `-`, prefixed by non-system base type and interface keys joined with `.`, e.g. `Sales-Messages-OrderPlaced`; subscribers bind `<key>.#` |

- **Publish target is explicit, never derived from the message type.** The user supplies `topic` per call or via the NServiceBus convention's `topics` registry (keyed by message type name); otherwise the convention throws `UnknownPublishTopicError` at `publish` time, before commit. The value is native to the topology and passed through as-is, so switching topologies means changing the topic values.
- A mistyped topic is **not** detected: publishes are not `mandatory`, so the event is dropped silently, same as publishing with no subscribers.
- **Conventional publish to a missing exchange**: NServiceBus subscribers create the event exchange when subscribing (NServiceBus' own publishers also declare it on every publish). A missing exchange therefore means nobody subscribed: by default the event is skipped like any event without subscribers, and the exchange is not created. With `createResources: true` it's declared (fanout, durable) on first publish, mirroring NServiceBus publishers. Existence is checked with a passive declare and cached per connection.
- Conventional send: the destination exchange is created by the receiving endpoint; if missing, dispatch fails and the control message retries.
- **Message type inheritance is out of scope**: Zusammen does not create or bind base-type/interface exchanges. Subscribers to a base type or interface will not receive Zusammen events (conventional); for direct, the topic must include the base type prefix NServiceBus generates.
- `ConversationId` defaults to the session ID, `CorrelationId` to the message ID; both can be overridden. `NServiceBus.Version` is not set.

### Receiver side (`Zusammen.NServiceBus`, NuGet)

A small .NET package for NServiceBus 10 endpoints receiving Zusammen messages, enabled with `endpointConfiguration.EnableZusammen()`, in `dotnet/src/Zusammen.NServiceBus`:

- A behavior in the `IIncomingPhysicalMessageContext` stage (runs before deserialization): if `NServiceBus.EnclosedMessageTypes` is missing and `zusammen.message-type` is present, resolve the .NET type and set `EnclosedMessageTypes` to its FullName. Messages that already carry `EnclosedMessageTypes` are untouched; the behavior is idempotent across retries.
- Type resolution: overrides (`EnableZusammen().MapMessageType<T>("Name")`) first, then the endpoint's message types by FullName, then by simple `Type.Name`. Message types sharing a simple name without an override → startup failure. Unknown name at runtime → exception → normal NServiceBus recoverability.

### Compatibility tests

`dotnet/compat/Zusammen.Compat.Endpoint` is a real NServiceBus 10.2 endpoint (RabbitMQ transport 11.2, MongoDB persistence) started by the Node.js integration tests, reporting handled and failed messages on stdout. Covered for both topologies: mapped commands (properties bound, headers), mapped events delivered to subscribers, unmapped messages resolved by `Zusammen.NServiceBus`, unmapped messages failing on endpoints without it; plus the NServiceBus outbox deduplicating a re-dispatched message. CI installs .NET and sets `ZUSAMMEN_REQUIRE_DOTNET=true` so these tests can't be skipped silently.

## Integrations

### Web frameworks

| Tier | Framework   | Package             | Integration point                                                      |
| ---- | ----------- | ------------------- | ---------------------------------------------------------------------- |
| 1    | Express v5  | `@zusammen/express` | middleware                                                             |
| 1    | Fastify v5  | `@zusammen/fastify` | plugin (`onRequest` / `preSerialization` / `onError` hooks)            |
| 1    | NestJS      | `@zusammen/nestjs`  | `ZusammenModule`, interceptor, injectable session (Express or Fastify) |
| 2    | Hono        | `@zusammen/hono`    | middleware                                                             |
| —    | Koa, others | none                | use the core `withSession` helper                                      |

All adapters are thin wrappers over core:

- `withSession(fn)` opens a session, runs `fn`, commits on success, rolls back on throw.
- The current session lives in `AsyncLocalStorage`; `getSession()` returns it anywhere in the call stack, adapters also expose it on the request/context object.
- **Commit before the response is sent.** Committing after the response would report success for a commit that can still fail (e.g. tombstone conflict). Adapters commit in a hook that runs before serialization/sending, and map commit failures to an error response.
- Opt-in per route (not every request needs a session); a global mode is available.

### Deployment

- At least one started factory must be processing the control queue. In the common deployment every web instance does it (every sender is a processor).
- Instances can stop or crash at any time after commit; pending control messages are handled by any other instance, or by the next one to start.
- Serverless deployments with RabbitMQ are not supported for now (decision 13).

## NServiceBus TransactionalSession Conformance

The [NServiceBus TransactionalSession acceptance tests](https://github.com/Particular/NServiceBus.TransactionalSession/tree/main/src/NServiceBus.TransactionalSession.AcceptanceTests) can't run against Zusammen (they drive the .NET API, depend on NServiceBus testing persistence and pipeline internals). Instead they serve as a behavioral spec:

- `docs/nservicebus-conformance.md` maps every upstream `When_*.cs` test to a Zusammen test or marks it not applicable, with a reason.
- A scheduled CI job lists the upstream test files (`gh api`) and fails if any is missing from the matrix, forcing a decision whenever NServiceBus adds behavior.

## Dependencies

- `@zusammen/core`: none
- `@zusammen/mongodb`: `mongodb` (peer dependency, current major)
- `@zusammen/rabbitmq`: `amqplib` (peer dependency)
- `@zusammen/nservicebus`: `@zusammen/core` (peer dependency) only
- Framework adapters: the framework itself as a peer dependency (`express`, `fastify`, `@nestjs/common` + `@nestjs/core`, `hono`)
- `Zusammen.NServiceBus` (NuGet): `NServiceBus`
- Compat tests: .NET 10 SDK, `NServiceBus`, `NServiceBus.RabbitMQ` (test-only, under `compat/`)
- Dev: `typescript`, `vitest`, `eslint`, `prettier`, `testcontainers` (MongoDB single-node replica set + RabbitMQ)

## Repository Setup

GitHub: [mauroservienti/Zusammen](https://github.com/mauroservienti/Zusammen) (public). Deferred until the repository is more stable (branch protection would slow down the remaining phases); until then, branches are squash-merged locally and pushed to `main`:

- **Protect `main`**: require a pull request before merging, require the CI status checks (`Node 22`, `Node 24`, `Node 26`) to pass and the branch to be up to date, require linear history, block force pushes and deletion. Apply to administrators too, so the rules can't be bypassed by accident.
- **Squash merges only**: disable merge commits and rebase merging in the repository settings; use the PR title and description as the squash commit message; automatically delete head branches after merge.
- **Workflow change**: from then on, work happens on branches pushed to GitHub, merged through a squash-merged PR (`gh pr create`, `gh pr merge --squash`) once CI is green, instead of local squash merges.
- **Dependency updates**: enable Dependabot (npm, GitHub Actions) with grouped weekly updates.
- **License**: still to be decided; add `LICENSE` and the `license` field to every package before the first npm publish.

## Implementation Phases

1. **Scaffold** — pnpm workspace, TypeScript project references, Vitest, ESLint/Prettier, `docker-compose.yml` (MongoDB replica set + RabbitMQ) for local dev, CI workflow, scheduled NServiceBus conformance drift check (see below).
2. **Core contracts** — types, interfaces (`PersistenceProvider`, `TransportProvider`, `MessageConvention`, `TransactionalSession`, `SessionFactory`), error classes, logger. Type-level tests (`expectTypeOf`) for the contracts.
3. **Core logic + default convention** — `TransactionalSession` (single use, `await using` rolls back), session factory (injectable clock and ID generator), `createControlMessageHandler` (window/backoff/tombstone/failures), Zusammen convention (type registry by constructor or class name, topics, pluggable serializer). Unit tests with in-memory fake providers covering every row of the decision table and every failure scenario.
4. **MongoDB provider** — `ClientSession` transactions on the application's `MongoClient`, document mapping (headers stored as key/value pairs because header names contain dots), write conflicts on the outbox insert mapped to `SessionCommitConflictError`, a shared persistence contract suite (`@zusammen/testing`, private) run against the in-memory fake and MongoDB, primary/`majority` read and write concerns on the control path, outbox collection, indexes (TTL), tombstone via duplicate key detection, mapping duplicate key on commit to `SessionCommitConflictError`.
5. **RabbitMQ provider** — `RabbitMQTransport` with amqplib 2 recovery, `RoutingTopology` contract + Zusammen topology, confirm channels, control/retry/error queues, `mandatory` + returns handling for sends, consumer with prefetch, `ControlResult` → ack/delay/dead-letter mapping.
6. **Integration tests (Testcontainers)** — private `@zusammen/integration-tests` package running the scenarios below on MongoDB 8 + RabbitMQ 4 with fault injection around the real providers (failed dispatches, crashes before commit, slow outbox writes and commits, a dying instance), plus concurrent sessions on competing instances, using the default wire format. Note for users: collections written inside sessions should exist beforehand, MongoDB can't implicitly create the same collection from concurrent transactions.
7. **Framework integrations** — core `withSession`, `getSession`/`tryGetSession` (`AsyncLocalStorage`), `settleSession` and `TransactionalSession.status`; Express 5 middleware (holds the first response write until the session is settled, replays it afterwards; on commit failure clears the handler's headers and responds 500 or via `onCommitError`), Fastify 5 plugin (`onRequest`/`onSend`, per-route opt-in or `global`), NestJS 12 module (global interceptor, `@Transactional()`, `@CurrentSession()`, `SessionAccessor`, factory lifecycle; handlers using `@Res()` are not supported), Hono 4 middleware. Tests per adapter: commit on success before the response, rollback on error, commit failure mapped to an error response, session reachable via `getSession()` and the request object.
8. **NServiceBus compatibility (opt-in)** — wire details verified against the NServiceBus source (see above); `@zusammen/nservicebus` convention (headers, `TimeSent` wire format, PascalCase JSON, explicit publish topics, `EnclosedMessageTypes` only for mapped types); `@zusammen/rabbitmq/nservicebus` conventional and direct topologies (AMQP properties as NServiceBus sets them, optional event exchanges, `validateConvention`); `Zusammen.NServiceBus` NuGet package with unit tests; compatibility tests against a real NServiceBus endpoint for both topologies, including outbox deduplication.
9. **Docs & samples** — guides in `docs/guides` (delivery guarantees and receiver idempotency, message types including bundlers and NServiceBus mapping, operations), a README per package (npm and NuGet), and two runnable samples on `docker-compose.yml`: `samples/node-to-node` (Express API, idempotent amqplib worker) and `samples/node-to-nservicebus` (Express API, NServiceBus 10 Billing endpoint receiving a mapped event and an unmapped command). Samples are TypeScript run with Node's type stripping, typechecked and linted in CI; the .NET sample is built in CI.

## Verification Scenarios

Core (default wire format):

- Happy path: commit succeeds, messages dispatched once, record marked dispatched.
- Rollback: nothing stored or dispatched, no control message sent.
- Crash before commit (after the control message was sent): the control message stores a tombstone once the window expires; nothing dispatched.
- Crash after commit (simulate by skipping immediate dispatch): control message dispatches.
- Control message before commit: delayed retries, then dispatch.
- Commit slower than window: tombstone stored, commit throws `SessionCommitConflictError`, no business data persisted.
- Immediate dispatch failure: control message dispatches.
- Persistent dispatch failure: control message ends in the error queue.
- Unresolvable message type: `send`/`publish` throws before commit.
- Send to a non-existent queue: dispatch fails (returned/unroutable), control message retries, then error queue.
- Publish with no subscribers: succeeds, record marked dispatched.
- Re-dispatch by the control message produces identical message ID, headers and body.
- Empty session: commit sends no control message and stores no outbox record.
- Committed record not yet visible to the control handler (storage lag): handled as "commit pending", never as "missing" after a successful commit.
- Two factories in one process with separate control queues: one commits, the other rolls back, independently.
- `open()` before `start()` throws.
- Process opening the session dies right after commit: another instance processes the control message and dispatches.
- Web adapter: handler throws → rollback, no messages; commit fails → error response, client never sees success.
- Concurrency: many sessions in parallel, multiple consumers; every committed message dispatched at least once.

NServiceBus (opt-in):

- NServiceBus receives a sent command: handler invoked with correct type and property values (PascalCase binding), for both topologies.
- NServiceBus subscriber receives a published event, for both topologies.
- NServiceBus receiver with Outbox enabled deduplicates a message dispatched twice (same `message_id`).
- Unmapped .NET type with the `Zusammen.NServiceBus` behavior installed: send and publish are handled with the correct type.
- Unmapped .NET type without the behavior: message ends in the NServiceBus error queue.
- Explicit .NET type mapping: `EnclosedMessageTypes` set, works with and without the behavior.
- Behavior startup fails on ambiguous simple type names.
- Publish without a topic (option or registry) under an NServiceBus topology: `publish` throws before commit.
- NServiceBus topology combined with the default convention: factory fails at startup.
