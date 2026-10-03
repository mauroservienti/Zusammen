import { hostname } from 'node:os';
import {
  classNameOf,
  resolveMessageType,
  UnknownMessageTypeError,
  UnknownPublishTopicError,
  ZusammenHeaders,
  type ConventionContext,
  type MessageConvention,
  type MessageTypeRegistry,
  type OutgoingMessage,
  type Serializer,
  type TransportOperation,
} from '@zusammen/core';
import { nserviceBusJsonSerializer, type PropertyNaming } from './serializer.js';
import { toWireFormat } from './time-sent.js';

export const NSERVICEBUS_CONVENTION = 'nservicebus';

export const NServiceBusHeaders = {
  messageId: 'NServiceBus.MessageId',
  enclosedMessageTypes: 'NServiceBus.EnclosedMessageTypes',
  contentType: 'NServiceBus.ContentType',
  messageIntent: 'NServiceBus.MessageIntent',
  timeSent: 'NServiceBus.TimeSent',
  conversationId: 'NServiceBus.ConversationId',
  correlationId: 'NServiceBus.CorrelationId',
  originatingEndpoint: 'NServiceBus.OriginatingEndpoint',
  originatingMachine: 'NServiceBus.OriginatingMachine',
  replyToAddress: 'NServiceBus.ReplyToAddress',
} as const;

export interface NServiceBusConventionOptions {
  /** Logical name of this application, sent as `NServiceBus.OriginatingEndpoint`. */
  endpointName: string;
  /**
   * .NET FullNames (e.g. `Sales.Messages.OrderPlaced`), keyed by constructor or class name. Unmapped messages carry
   * only `zusammen.message-type`, and receivers need the `Zusammen.NServiceBus` package to resolve them.
   */
  messageTypes?: MessageTypeRegistry | undefined;
  /**
   * Publish topics keyed by message type name (the .NET FullName when mapped, otherwise the class name): the
   * exchange name (`Namespace:TypeName`) for the conventional topology, the routing key for the direct topology.
   */
  topics?: Readonly<Record<string, string>> | undefined;
  /** Queue NServiceBus endpoints reply to, sent as `NServiceBus.ReplyToAddress`. */
  replyToAddress?: string | undefined;
  /** Defaults to PascalCase property names, matching NServiceBus' default System.Text.Json settings. */
  propertyNaming?: PropertyNaming | undefined;
  /** Replaces the JSON serializer; its content type is sent as `NServiceBus.ContentType`. */
  serializer?: Serializer | undefined;
  /** Defaults to the host name. */
  machineName?: string | undefined;
}

// Headers the convention owns; user headers can't override them
const PROTECTED_HEADERS = new Set<string>([
  NServiceBusHeaders.messageId,
  NServiceBusHeaders.messageIntent,
  NServiceBusHeaders.enclosedMessageTypes,
  NServiceBusHeaders.contentType,
  ZusammenHeaders.messageType,
]);

/** Produces messages NServiceBus endpoints can consume (native integration). */
export function nserviceBusConvention(options: NServiceBusConventionOptions): MessageConvention {
  const serializer = options.serializer ?? nserviceBusJsonSerializer(options.propertyNaming);
  const machineName = options.machineName ?? hostname();

  return {
    name: NSERVICEBUS_CONVENTION,
    toOperation(input: OutgoingMessage, context: ConventionContext): TransportOperation {
      const enclosedMessageType = resolveMessageType(input.message, input.options.messageType, options.messageTypes);
      const messageType = enclosedMessageType ?? classNameOf(input.message);
      if (messageType === undefined) {
        throw new UnknownMessageTypeError(
          NSERVICEBUS_CONVENTION,
          'pass the messageType option (a .NET FullName), register the type, or send a class instance',
        );
      }

      let topic: string | undefined;
      if (input.intent === 'publish') {
        topic = input.options.topic ?? options.topics?.[messageType];
        if (topic === undefined) {
          throw new UnknownPublishTopicError(messageType);
        }
      }

      const messageId = input.options.messageId ?? context.newId();
      const correlationId = input.options.correlationId ?? messageId;
      const headers: Record<string, string> = {
        [NServiceBusHeaders.conversationId]: context.sessionId,
        [NServiceBusHeaders.correlationId]: correlationId,
        [NServiceBusHeaders.originatingEndpoint]: options.endpointName,
        [NServiceBusHeaders.originatingMachine]: machineName,
        ...(options.replyToAddress !== undefined && { [NServiceBusHeaders.replyToAddress]: options.replyToAddress }),
        ...Object.fromEntries(
          Object.entries(input.options.headers ?? {}).filter(([name]) => !PROTECTED_HEADERS.has(name)),
        ),
        [NServiceBusHeaders.messageId]: messageId,
        [NServiceBusHeaders.messageIntent]: input.intent === 'send' ? 'Send' : 'Publish',
        [NServiceBusHeaders.timeSent]: toWireFormat(context.now()),
        [NServiceBusHeaders.contentType]: serializer.contentType,
        [ZusammenHeaders.messageType]: messageType,
        ...(enclosedMessageType !== undefined && { [NServiceBusHeaders.enclosedMessageTypes]: enclosedMessageType }),
      };

      const operation: TransportOperation = {
        messageId,
        intent: input.intent,
        messageType,
        headers,
        body: serializer.serialize(input.message),
        properties: { contentType: serializer.contentType, correlationId },
      };
      if (options.replyToAddress !== undefined) operation.properties.replyTo = options.replyToAddress;
      if (input.intent === 'send') operation.destination = input.destination;
      if (topic !== undefined) operation.topic = topic;
      return operation;
    },
  };
}
