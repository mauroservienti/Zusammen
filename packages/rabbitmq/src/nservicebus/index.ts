import type { Options } from 'amqplib';
import { IncompatibleConventionError, type MessageConvention, type TransportOperation } from '@zusammen/core';
import type { RoutingTopology } from '../topology.js';

const NSERVICEBUS_CONVENTION = 'nservicebus';
const AMQP_TOPIC_EXCHANGE = 'amq.topic';

function validateConvention(convention: MessageConvention): void {
  if (convention.name !== NSERVICEBUS_CONVENTION) {
    throw new IncompatibleConventionError(
      convention.name,
      'the NServiceBus routing topologies require nserviceBusConvention() from @zusammen/nservicebus',
    );
  }
}

/** AMQP properties as NServiceBus.RabbitMQ sets them (BasicPropertiesExtensions.Fill). */
function publishOptions(operation: TransportOperation): Options.Publish {
  const headers = operation.headers;
  const options: Options.Publish = {
    messageId: operation.messageId,
    persistent: true,
    headers: { ...headers },
    contentType: headers['NServiceBus.ContentType'] ?? 'application/octet-stream',
  };
  const correlationId = headers['NServiceBus.CorrelationId'];
  if (correlationId !== undefined) options.correlationId = correlationId;
  const replyTo = headers['NServiceBus.ReplyToAddress'];
  if (replyTo !== undefined) options.replyTo = replyTo;
  // Only from EnclosedMessageTypes: NServiceBus copies `type` into a missing EnclosedMessageTypes header on receive,
  // which would hide unmapped types from the Zusammen.NServiceBus receiver behavior
  const enclosedMessageTypes = headers['NServiceBus.EnclosedMessageTypes'];
  const type = enclosedMessageTypes?.split(',')[0];
  if (type !== undefined) options.type = type;
  return options;
}

/**
 * NServiceBus conventional routing: a fanout exchange per endpoint and per event type. Sends go to the destination
 * endpoint's exchange; publishes go to the exchange named by the topic (`Namespace:TypeName`), which NServiceBus
 * subscribers create. Without subscribers the exchange may not exist and the event is skipped, or the exchange is
 * declared when resource creation is enabled.
 */
export function nserviceBusConventionalTopology(): RoutingTopology {
  return {
    name: 'nservicebus-conventional',
    requiredExchanges: () => [],
    createResources: () => Promise.resolve(),
    route(operation) {
      if (operation.intent === 'send') {
        return { exchange: operation.destination ?? '', routingKey: '' };
      }
      return { exchange: operation.topic ?? '', routingKey: '', optionalExchange: { type: 'fanout' } };
    },
    validateConvention,
    publishOptions,
  };
}

/**
 * NServiceBus direct routing: sends go to the destination queue through the default exchange; publishes go to
 * `amq.topic` with the topic as routing key (`Namespace-TypeName`, prefixed by base types, as NServiceBus generates
 * it).
 */
export function nserviceBusDirectTopology(): RoutingTopology {
  return {
    name: 'nservicebus-direct',
    requiredExchanges: () => [],
    createResources: () => Promise.resolve(),
    route(operation) {
      if (operation.intent === 'send') {
        return { exchange: '', routingKey: operation.destination ?? '' };
      }
      return { exchange: AMQP_TOPIC_EXCHANGE, routingKey: operation.topic ?? '' };
    },
    validateConvention,
    publishOptions,
  };
}
