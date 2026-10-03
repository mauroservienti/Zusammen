import { describe, expect, test } from 'vitest';
import {
  UnknownMessageTypeError,
  zusammenConvention,
  ZusammenHeaders,
  type ConventionContext,
  type OutgoingMessage,
} from '@zusammen/core';

class OrderPlaced {
  constructor(readonly orderId: string) {}
}

const context: ConventionContext = {
  sessionId: 'session-1',
  now: () => new Date('2026-10-03T12:00:00.123Z'),
  newId: () => 'generated-id',
};

const publish = (message: unknown, options: Extract<OutgoingMessage, { intent: 'publish' }>['options'] = {}) =>
  ({ intent: 'publish', message, options }) as const;

describe('zusammen convention', () => {
  test('builds the default wire format', () => {
    const operation = zusammenConvention().toOperation(
      { intent: 'send', destination: 'billing', message: new OrderPlaced('o1'), options: {} },
      context,
    );

    expect(operation).toEqual({
      messageId: 'generated-id',
      intent: 'send',
      destination: 'billing',
      messageType: 'OrderPlaced',
      headers: {
        'zusammen.message-id': 'generated-id',
        'zusammen.message-type': 'OrderPlaced',
        'zusammen.intent': 'send',
        'zusammen.time-sent': '2026-10-03T12:00:00.123Z',
        'zusammen.session-id': 'session-1',
      },
      body: new TextEncoder().encode('{"orderId":"o1"}'),
      properties: { contentType: 'application/json' },
    });
  });

  test('message type resolution: option, then registry, then class name', () => {
    const byName = zusammenConvention({ messageTypes: { OrderPlaced: 'sales.order-placed' } });
    const byConstructor = zusammenConvention({ messageTypes: new Map([[OrderPlaced, 'sales.by-constructor']]) });

    expect(byName.toOperation(publish(new OrderPlaced('o1'), { messageType: 'explicit' }), context).messageType).toBe(
      'explicit',
    );
    expect(byName.toOperation(publish(new OrderPlaced('o1')), context).messageType).toBe('sales.order-placed');
    expect(byConstructor.toOperation(publish(new OrderPlaced('o1')), context).messageType).toBe('sales.by-constructor');
    expect(zusammenConvention().toOperation(publish(new OrderPlaced('o1')), context).messageType).toBe('OrderPlaced');
  });

  test.each([
    ['a plain object', { orderId: 'o1' }],
    // eslint-disable-next-line @typescript-eslint/no-extraneous-class
    ['an anonymous class instance', new (class {})()],
    ['a primitive', 'text'],
    ['null', null],
  ])('cannot resolve a type for %s', (_, message) => {
    expect(() => zusammenConvention().toOperation(publish(message), context)).toThrow(UnknownMessageTypeError);
  });

  test('plain objects work with an explicit type', () => {
    const operation = zusammenConvention().toOperation(
      publish({ orderId: 'o1' }, { messageType: 'OrderPlaced' }),
      context,
    );
    expect(operation.messageType).toBe('OrderPlaced');
  });

  test('publish topic: option, then registry, else left to the topology', () => {
    const convention = zusammenConvention({ topics: { OrderPlaced: 'sales.orders' } });

    expect(convention.toOperation(publish(new OrderPlaced('o1'), { topic: 'explicit' }), context).topic).toBe(
      'explicit',
    );
    expect(convention.toOperation(publish(new OrderPlaced('o1')), context).topic).toBe('sales.orders');
    expect(zusammenConvention().toOperation(publish(new OrderPlaced('o1')), context)).not.toHaveProperty('topic');
  });

  test('user headers are kept but cannot override convention headers', () => {
    const operation = zusammenConvention().toOperation(
      publish(new OrderPlaced('o1'), {
        headers: { tenant: 'acme', [ZusammenHeaders.messageType]: 'spoofed', [ZusammenHeaders.intent]: 'send' },
      }),
      context,
    );

    expect(operation.headers).toMatchObject({
      tenant: 'acme',
      'zusammen.message-type': 'OrderPlaced',
      'zusammen.intent': 'publish',
    });
  });

  test('explicit message ID and correlation ID', () => {
    const operation = zusammenConvention().toOperation(
      publish(new OrderPlaced('o1'), { messageId: 'm-1', correlationId: 'c-1' }),
      context,
    );

    expect(operation.messageId).toBe('m-1');
    expect(operation.headers[ZusammenHeaders.messageId]).toBe('m-1');
    expect(operation.properties.correlationId).toBe('c-1');
  });

  test('custom serializer', () => {
    const operation = zusammenConvention({
      serializer: { contentType: 'text/plain', serialize: () => new Uint8Array([1, 2, 3]) },
    }).toOperation(publish(new OrderPlaced('o1')), context);

    expect(operation.body).toEqual(new Uint8Array([1, 2, 3]));
    expect(operation.properties.contentType).toBe('text/plain');
  });
});
