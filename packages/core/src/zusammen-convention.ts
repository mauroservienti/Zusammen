import type { ConventionContext, MessageConvention, OutgoingMessage } from './convention.js';
import { UnknownMessageTypeError } from './errors.js';
import type { TransportOperation } from './outbox.js';

export const ZusammenHeaders = {
  messageId: 'zusammen.message-id',
  messageType: 'zusammen.message-type',
  intent: 'zusammen.intent',
  timeSent: 'zusammen.time-sent',
  sessionId: 'zusammen.session-id',
} as const;

export interface Serializer {
  readonly contentType: string;
  serialize(message: unknown): Uint8Array;
}

export const jsonSerializer: Serializer = {
  contentType: 'application/json',
  serialize: (message) => new TextEncoder().encode(JSON.stringify(message)),
};

// eslint-disable-next-line @typescript-eslint/no-explicit-any -- any constructor, whatever its parameters
export type MessageConstructor = abstract new (...args: any[]) => unknown;

/** Message type names, keyed by constructor (robust to minification) or by class name. */
export type MessageTypeRegistry = ReadonlyMap<MessageConstructor, string> | Readonly<Record<string, string>>;

export interface ZusammenConventionOptions {
  messageTypes?: MessageTypeRegistry | undefined;
  /** Publish topics keyed by message type name. Without one, the routing topology decides (default: the message type). */
  topics?: Readonly<Record<string, string>> | undefined;
  serializer?: Serializer | undefined;
}

/**
 * Resolves a message type: explicit option, then registry, then optionally the class name.
 * Shared with other conventions so resolution rules stay consistent.
 */
export function resolveMessageType(
  message: unknown,
  explicit: string | undefined,
  registry: MessageTypeRegistry | undefined,
): string | undefined {
  if (explicit !== undefined) {
    return explicit;
  }
  const constructor = constructorOf(message);
  if (constructor === undefined) {
    return undefined;
  }
  if (registry instanceof Map) {
    const byConstructor = (registry as ReadonlyMap<MessageConstructor, string>).get(constructor);
    if (byConstructor !== undefined) {
      return byConstructor;
    }
  } else if (registry !== undefined && Object.hasOwn(registry, constructor.name)) {
    return (registry as Readonly<Record<string, string>>)[constructor.name];
  }
  return undefined;
}

/** The class name of a class instance; undefined for plain objects, primitives and anonymous classes. */
export function classNameOf(message: unknown): string | undefined {
  const name = constructorOf(message)?.name;
  return name === undefined || name === '' || name === 'Object' ? undefined : name;
}

function constructorOf(message: unknown): MessageConstructor | undefined {
  if (typeof message !== 'object' || message === null) {
    return undefined;
  }
  const prototype = Object.getPrototypeOf(message) as { constructor?: unknown } | null;
  const constructor = prototype?.constructor;
  return typeof constructor === 'function' ? (constructor as MessageConstructor) : undefined;
}

/** The default convention: a minimal, library-agnostic wire format. */
export function zusammenConvention(options: ZusammenConventionOptions = {}): MessageConvention {
  const serializer = options.serializer ?? jsonSerializer;

  return {
    name: 'zusammen',
    toOperation(input: OutgoingMessage, context: ConventionContext): TransportOperation {
      const messageType =
        resolveMessageType(input.message, input.options.messageType, options.messageTypes) ??
        classNameOf(input.message);
      if (messageType === undefined) {
        throw new UnknownMessageTypeError(
          'zusammen',
          'pass the messageType option, register the type, or send a class instance',
        );
      }

      const messageId = input.options.messageId ?? context.newId();
      const headers: Record<string, string> = {
        ...input.options.headers,
        [ZusammenHeaders.messageId]: messageId,
        [ZusammenHeaders.messageType]: messageType,
        [ZusammenHeaders.intent]: input.intent,
        [ZusammenHeaders.timeSent]: context.now().toISOString(),
        [ZusammenHeaders.sessionId]: context.sessionId,
      };

      const operation: TransportOperation = {
        messageId,
        intent: input.intent,
        messageType,
        headers,
        body: serializer.serialize(input.message),
        properties: { contentType: serializer.contentType },
      };
      if (input.options.correlationId !== undefined) {
        operation.properties.correlationId = input.options.correlationId;
      }
      if (input.intent === 'send') {
        operation.destination = input.destination;
      } else {
        const topic = input.options.topic ?? options.topics?.[messageType];
        if (topic !== undefined) {
          operation.topic = topic;
        }
      }
      return operation;
    },
  };
}
