import { describe, expect, expectTypeOf, test } from 'vitest';
import type {
  ControlResult,
  OutgoingMessage,
  PersistenceProvider,
  PublishOptions,
  SendOptions,
  TransactionalSession,
  TransportOperation,
} from '@zusammen/core';
import { DEFAULT_MAX_COMMIT_DURATION_MS, silentLogger } from '@zusammen/core';

describe('contracts', () => {
  test('outgoing messages are discriminated by intent', () => {
    const narrow = (input: OutgoingMessage) => (input.intent === 'send' ? input.destination : input.options.topic);
    expectTypeOf(narrow).returns.toEqualTypeOf<string | undefined>();
  });

  test('only publish options carry a topic', () => {
    expectTypeOf<PublishOptions>().toHaveProperty('topic');
    expectTypeOf<SendOptions>().not.toHaveProperty('topic');
  });

  test('retry results carry the next control message', () => {
    const next = (result: ControlResult) => (result.kind === 'retry' ? result.next.attempt : undefined);
    expectTypeOf(next).returns.toEqualTypeOf<number | undefined>();
  });

  test('sessions expose the persistence transaction context', () => {
    type MongoLikeSession = { id: string };
    type Persistence = PersistenceProvider<MongoLikeSession>;
    expectTypeOf<Parameters<Persistence['commit']>[0]>().toEqualTypeOf<MongoLikeSession>();
    expectTypeOf<TransactionalSession<MongoLikeSession>['transactionContext']>().toEqualTypeOf<MongoLikeSession>();
    expectTypeOf<TransactionalSession<MongoLikeSession>>().toExtend<AsyncDisposable>();
  });

  test('operations are binary and keep stable ids', () => {
    expectTypeOf<TransportOperation['body']>().toEqualTypeOf<Uint8Array>();
    expectTypeOf<TransportOperation['messageId']>().toBeString();
  });

  test('defaults', () => {
    expect(DEFAULT_MAX_COMMIT_DURATION_MS).toBe(15_000);
    expect(() => {
      silentLogger.error('ignored', { any: 'thing' });
    }).not.toThrow();
  });
});
