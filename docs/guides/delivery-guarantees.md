# Delivery guarantees

Zusammen makes one promise: **business data and outgoing messages are committed together, or not at all**. This guide explains what that means for the code receiving the messages. For the background, see [What's an Outbox and why do we need it?](https://milestone.topics.it/2023/02/07/outbox-what-and-why.html).

## What you get

| Guarantee              | Meaning                                                                                                                 |
| ---------------------- | ----------------------------------------------------------------------------------------------------------------------- |
| Atomic state change    | If the transaction commits, its messages will be sent. If it rolls back, none are sent.                                 |
| At-least-once dispatch | Every committed message reaches the broker at least once, even if the process dies right after the commit.              |
| Stable messages        | A message re-sent by the control message is byte-identical to the first copy: same message ID, same headers, same body. |

## What you don't get

- **Exactly-once delivery.** A message can be dispatched more than once, for example when the process crashes after dispatching but before recording that it did, or when a broker confirm is lost. Duplicates are rare in normal operation (the control message is delayed so it doesn't race the immediate dispatch), but receivers must handle them.
- **Ordering.** Messages from one session are dispatched concurrently; messages from different sessions are independent. If the order matters, carry what's needed to reorder (e.g. a version number) in the message.
- **Delivery to subscribers that don't exist yet.** Events published before anyone subscribed are dropped, exactly like with any pub/sub broker setup.
- **Protection for other side effects.** Only the database writes enlisted in the session and the outgoing messages are atomic. An HTTP call, an email or a file written during the session happens even if the session rolls back, and can't be undone. Move such side effects to the receiving side of a message, and design them to tolerate retries: an idempotent API (e.g. with an idempotency key), or a process that tries only once and handles a timeout as a business case rather than retrying blindly.

## Making receivers idempotent

At-least-once delivery plus deduplication on the receiving side gives **exactly-once processing**: a message may arrive several times, but its effects are applied once. The receiving side keeps track of the message IDs it processed, an _inbox_. Pick the cheapest option that fits the handler:

1. **Naturally idempotent operations.** "Set the order status to shipped" is idempotent; "add 10 to the balance" is not.
2. **Deduplicate by message ID.** Every message carries a stable ID (AMQP `message_id`, plus `zusammen.message-id` or `NServiceBus.MessageId`). Store processed IDs in the same transaction as the handler's changes, e.g. a collection with a unique index on the message ID, and skip messages already seen. The [Node → Node sample](../../samples/node-to-node) does this.
3. **NServiceBus receivers: enable the [outbox](https://docs.particular.net/nservicebus/outbox/).** It deduplicates by `NServiceBus.MessageId` and makes the handler's outgoing messages consistent too. Zusammen's compatibility tests verify that the NServiceBus outbox drops a message Zusammen dispatched again.

## How the guarantee is kept

On `commit()`, Zusammen sends a delayed **control message** to the broker, then stores the outgoing messages in an outbox record within the business transaction, commits, and dispatches immediately. Whichever instance receives the control message later checks the outbox:

- record dispatched → nothing to do;
- record not dispatched (e.g. the committing process died) → dispatch it;
- no record yet → wait and retry, within the commit window (default 15 s);
- still no record after the window → store a _tombstone_, so a commit that arrives even later fails with `SessionCommitConflictError` instead of committing messages nobody would dispatch.

The [implementation plan](../node-js-transactional-session-implementation.md) has the full decision table and failure analysis.
