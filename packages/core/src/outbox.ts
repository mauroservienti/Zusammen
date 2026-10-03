/** How an outgoing message is routed: to a single destination, or to whoever subscribed to a topic. */
export type MessageIntent = 'send' | 'publish';

/** Native message properties every transport knows how to map (e.g. AMQP `content_type`, `correlation_id`, `reply_to`). */
export interface TransportOperationProperties {
  contentType: string;
  correlationId?: string;
  replyTo?: string;
}

/**
 * A fully prepared outgoing message. Produced by the {@link MessageConvention} at `send`/`publish` time and stored
 * as-is in the outbox, so every (re-)dispatch puts byte-identical messages on the wire.
 */
export interface TransportOperation {
  /** Stable across re-dispatches; receivers use it for deduplication. */
  messageId: string;
  intent: MessageIntent;
  /** Send only: the logical destination, interpreted by the transport's routing topology. */
  destination?: string;
  /** Publish only: where to publish, interpreted by the transport's routing topology. */
  topic?: string;
  /** Message type name as resolved by the convention. */
  messageType: string;
  headers: Record<string, string>;
  body: Uint8Array;
  properties: TransportOperationProperties;
}

/** One outbox record per transactional session, keyed by the session ID. */
export interface OutboxRecord {
  /** The session ID. */
  id: string;
  dispatched: boolean;
  dispatchedAt?: Date;
  /** Empty for tombstones. */
  transportOperations: TransportOperation[];
}
