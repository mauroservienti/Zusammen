import type { MessageIntent, TransportOperation } from './outbox.js';

/** Options shared by `send` and `publish`. */
export interface OutgoingMessageOptions {
  /** Overrides the message type resolved by the convention. */
  messageType?: string | undefined;
  /** Overrides the generated message ID. */
  messageId?: string | undefined;
  /** Merged into the convention's headers; conventions protect the headers they own. */
  headers?: Record<string, string> | undefined;
  correlationId?: string | undefined;
}

export type SendOptions = OutgoingMessageOptions;

export interface PublishOptions extends OutgoingMessageOptions {
  /** Native publish target, interpreted by the transport's routing topology. */
  topic?: string | undefined;
}

/** A message as handed to `send`/`publish`, before the convention turns it into a {@link TransportOperation}. */
export type OutgoingMessage =
  | { intent: Extract<MessageIntent, 'send'>; destination: string; message: unknown; options: SendOptions }
  | { intent: Extract<MessageIntent, 'publish'>; message: unknown; options: PublishOptions };

/** Ambient information and services a convention may use; injectable for deterministic tests. */
export interface ConventionContext {
  sessionId: string;
  now(): Date;
  newId(): string;
}

/** Decides what goes on the wire: message type name, headers, body, native properties. Transport-agnostic. */
export interface MessageConvention {
  /** Identifies the convention, e.g. `zusammen` or `nservicebus`; transports use it to reject incompatible combinations. */
  readonly name: string;
  /** Runs at `send`/`publish` time. Throws (e.g. {@link UnknownMessageTypeError}) when the message can't be mapped. */
  toOperation(input: OutgoingMessage, context: ConventionContext): TransportOperation;
}
