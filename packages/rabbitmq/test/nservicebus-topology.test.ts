import { describe, expect, test } from 'vitest';
import { IncompatibleConventionError, zusammenConvention, type TransportOperation } from '@zusammen/core';
import { nserviceBusConvention } from '@zusammen/nservicebus';
import { nserviceBusConventionalTopology, nserviceBusDirectTopology } from '@zusammen/rabbitmq/nservicebus';

const operation = (overrides: Partial<TransportOperation> = {}): TransportOperation => ({
  messageId: 'm1',
  intent: 'send',
  destination: 'Sales',
  messageType: 'Sales.Messages.PlaceOrder',
  headers: {
    'NServiceBus.MessageId': 'm1',
    'NServiceBus.EnclosedMessageTypes': 'Sales.Messages.PlaceOrder, Sales.Messages',
    'NServiceBus.ContentType': 'application/json',
    'NServiceBus.CorrelationId': 'c1',
    'NServiceBus.ReplyToAddress': 'Sales.Api',
  },
  body: new Uint8Array(),
  properties: { contentType: 'application/json' },
  ...overrides,
});

describe('nservicebus topologies', () => {
  test('conventional: sends to the endpoint exchange, publishes to the topic exchange (optional)', () => {
    const topology = nserviceBusConventionalTopology();

    expect(topology.route(operation())).toEqual({ exchange: 'Sales', routingKey: '' });
    expect(
      topology.route(
        operation({ intent: 'publish', topic: 'Sales.Messages:OrderPlaced', destination: undefined as never }),
      ),
    ).toEqual({ exchange: 'Sales.Messages:OrderPlaced', routingKey: '', optionalExchange: { type: 'fanout' } });
  });

  test('direct: sends through the default exchange, publishes to amq.topic', () => {
    const topology = nserviceBusDirectTopology();

    expect(topology.route(operation())).toEqual({ exchange: '', routingKey: 'Sales' });
    expect(topology.route(operation({ intent: 'publish', topic: 'Sales-Messages-OrderPlaced' }))).toEqual({
      exchange: 'amq.topic',
      routingKey: 'Sales-Messages-OrderPlaced',
    });
  });

  test('AMQP properties mirror NServiceBus.RabbitMQ', () => {
    const options = nserviceBusConventionalTopology().publishOptions?.(operation());

    expect(options).toEqual({
      messageId: 'm1',
      persistent: true,
      contentType: 'application/json',
      correlationId: 'c1',
      replyTo: 'Sales.Api',
      type: 'Sales.Messages.PlaceOrder',
      headers: operation().headers,
    });
  });

  test('no AMQP type without EnclosedMessageTypes, so receivers resolve zusammen.message-type', () => {
    const options = nserviceBusDirectTopology().publishOptions?.(
      operation({ headers: { 'zusammen.message-type': 'PlaceOrder' } }),
    );

    expect(options).not.toHaveProperty('type');
    expect(options?.contentType).toBe('application/octet-stream');
  });

  test('require the NServiceBus convention', () => {
    for (const topology of [nserviceBusConventionalTopology(), nserviceBusDirectTopology()]) {
      expect(() => topology.validateConvention?.(zusammenConvention())).toThrow(IncompatibleConventionError);
      expect(() => topology.validateConvention?.(nserviceBusConvention({ endpointName: 'Sales.Api' }))).not.toThrow();
    }
  });
});
