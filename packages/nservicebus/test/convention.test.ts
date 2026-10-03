import { describe, expect, test } from 'vitest';
import {
  UnknownMessageTypeError,
  UnknownPublishTopicError,
  type ConventionContext,
  type OutgoingMessage,
} from '@zusammen/core';
import { nserviceBusConvention, nserviceBusJsonSerializer, toWireFormat } from '@zusammen/nservicebus';

class PlaceOrder {
  constructor(
    readonly orderId: string,
    readonly lines: { productId: string; quantity: number }[],
    readonly placedAt: Date,
  ) {}
}

class OrderPlaced {
  constructor(readonly orderId: string) {}
}

const context: ConventionContext = {
  sessionId: 'session-1',
  now: () => new Date('2026-10-03T12:34:56.789Z'),
  newId: () => 'message-1',
};

const decode = (body: Uint8Array) => new TextDecoder().decode(body);

const convention = nserviceBusConvention({
  endpointName: 'Sales.Api',
  machineName: 'web-1',
  messageTypes: new Map([[PlaceOrder, 'Sales.Messages.PlaceOrder']]),
  topics: { 'Sales.Messages.OrderPlaced': 'Sales.Messages:OrderPlaced', OrderPlaced: 'Sales.Messages:OrderPlaced' },
});

const send = (message: unknown, options: Extract<OutgoingMessage, { intent: 'send' }>['options'] = {}) =>
  ({ intent: 'send', destination: 'Sales', message, options }) as const;
const publish = (message: unknown, options: Extract<OutgoingMessage, { intent: 'publish' }>['options'] = {}) =>
  ({ intent: 'publish', message, options }) as const;

describe('nservicebus convention', () => {
  test('a mapped send carries the full NServiceBus header set and a PascalCase body', () => {
    const operation = convention.toOperation(
      send(new PlaceOrder('o1', [{ productId: 'p1', quantity: 2 }], new Date('2026-10-01T00:00:00.000Z'))),
      context,
    );

    expect(operation).toEqual({
      messageId: 'message-1',
      intent: 'send',
      destination: 'Sales',
      messageType: 'Sales.Messages.PlaceOrder',
      headers: {
        'NServiceBus.MessageId': 'message-1',
        'NServiceBus.EnclosedMessageTypes': 'Sales.Messages.PlaceOrder',
        'NServiceBus.ContentType': 'application/json',
        'NServiceBus.MessageIntent': 'Send',
        'NServiceBus.TimeSent': '2026-10-03 12:34:56:789000 Z',
        'NServiceBus.ConversationId': 'session-1',
        'NServiceBus.CorrelationId': 'message-1',
        'NServiceBus.OriginatingEndpoint': 'Sales.Api',
        'NServiceBus.OriginatingMachine': 'web-1',
        'zusammen.message-type': 'Sales.Messages.PlaceOrder',
      },
      body: expect.any(Uint8Array) as Uint8Array,
      properties: { contentType: 'application/json', correlationId: 'message-1' },
    });
    expect(JSON.parse(decode(operation.body))).toEqual({
      OrderId: 'o1',
      Lines: [{ ProductId: 'p1', Quantity: 2 }],
      PlacedAt: '2026-10-01T00:00:00.000Z',
    });
  });

  test('unmapped messages carry only zusammen.message-type, for the receiver to resolve', () => {
    const operation = convention.toOperation(publish(new OrderPlaced('o1')), context);

    expect(operation.headers).not.toHaveProperty('NServiceBus.EnclosedMessageTypes');
    expect(operation.headers['zusammen.message-type']).toBe('OrderPlaced');
    expect(operation.headers['NServiceBus.MessageIntent']).toBe('Publish');
    expect(operation.topic).toBe('Sales.Messages:OrderPlaced');
  });

  test('an explicit type maps plain objects', () => {
    const operation = convention.toOperation(
      publish({ orderId: 'o1' }, { messageType: 'Sales.Messages.OrderPlaced' }),
      context,
    );

    expect(operation.headers['NServiceBus.EnclosedMessageTypes']).toBe('Sales.Messages.OrderPlaced');
    expect(operation.topic).toBe('Sales.Messages:OrderPlaced');
  });

  test('publishes need a topic, known before commit', () => {
    const noTopics = nserviceBusConvention({ endpointName: 'Sales.Api' });

    expect(() => noTopics.toOperation(publish(new OrderPlaced('o1')), context)).toThrow(UnknownPublishTopicError);
    expect(noTopics.toOperation(publish(new OrderPlaced('o1'), { topic: 'explicit' }), context).topic).toBe('explicit');
  });

  test('plain objects without a type are rejected', () => {
    expect(() => convention.toOperation(send({ orderId: 'o1' }), context)).toThrow(UnknownMessageTypeError);
  });

  test('user headers are kept, protected headers are not overridable, conversation and correlation are', () => {
    const operation = convention.toOperation(
      send(new OrderPlaced('o1'), {
        correlationId: 'request-7',
        headers: {
          tenant: 'acme',
          'NServiceBus.ConversationId': 'conversation-9',
          'NServiceBus.MessageIntent': 'Publish',
          'NServiceBus.MessageId': 'spoofed',
          'NServiceBus.EnclosedMessageTypes': 'Spoofed',
        },
      }),
      context,
    );

    expect(operation.headers).toMatchObject({
      tenant: 'acme',
      'NServiceBus.ConversationId': 'conversation-9',
      'NServiceBus.CorrelationId': 'request-7',
      'NServiceBus.MessageIntent': 'Send',
      'NServiceBus.MessageId': 'message-1',
    });
    expect(operation.headers).not.toHaveProperty('NServiceBus.EnclosedMessageTypes');
    expect(operation.properties.correlationId).toBe('request-7');
  });

  test('reply-to address', () => {
    const operation = nserviceBusConvention({ endpointName: 'Sales.Api', replyToAddress: 'Sales.Api' }).toOperation(
      send(new OrderPlaced('o1')),
      context,
    );

    expect(operation.headers['NServiceBus.ReplyToAddress']).toBe('Sales.Api');
    expect(operation.properties.replyTo).toBe('Sales.Api');
  });

  test('property names can be preserved for receivers configured for camelCase', () => {
    const operation = nserviceBusConvention({ endpointName: 'Sales.Api', propertyNaming: 'preserve' }).toOperation(
      send(new OrderPlaced('o1')),
      context,
    );

    expect(decode(operation.body)).toBe('{"orderId":"o1"}');
  });

  test('serializer keeps arrays, nulls and primitives intact', () => {
    const body = nserviceBusJsonSerializer().serialize({ a: [1, 'x', null, { b: true }], c: null, d: 'text' });
    expect(decode(body)).toBe('{"A":[1,"x",null,{"B":true}],"C":null,"D":"text"}');
  });

  test.each([
    ['2026-10-03T12:34:56.789Z', '2026-10-03 12:34:56:789000 Z'],
    ['2026-01-02T03:04:05.006Z', '2026-01-02 03:04:05:006000 Z'],
    ['0999-12-31T23:59:59.000Z', '0999-12-31 23:59:59:000000 Z'],
  ])('TimeSent wire format for %s', (iso, wire) => {
    expect(toWireFormat(new Date(iso))).toBe(wire);
  });
});
