import { describe, expect, test } from 'vitest';
import {
  FactoryNotStartedError,
  IncompatibleConventionError,
  SessionClosedError,
  SessionCommitConflictError,
  UnknownMessageTypeError,
  UnknownPublishTopicError,
  UnroutableMessageError,
  ZusammenError,
} from '@zusammen/core';

describe('errors', () => {
  test.each([
    ['SessionCommitConflictError', new SessionCommitConflictError('s1')],
    ['SessionClosedError', new SessionClosedError('s1', 'committed')],
    ['FactoryNotStartedError', new FactoryNotStartedError()],
    ['UnknownMessageTypeError', new UnknownMessageTypeError('zusammen', 'plain object')],
    ['UnknownPublishTopicError', new UnknownPublishTopicError('OrderPlaced')],
    ['UnroutableMessageError', new UnroutableMessageError('m1', 'sales')],
    ['IncompatibleConventionError', new IncompatibleConventionError('zusammen', 'requires nservicebus')],
  ])('%s is a named ZusammenError', (name, error) => {
    expect(error).toBeInstanceOf(ZusammenError);
    expect(error).toBeInstanceOf(Error);
    expect(error.name).toBe(name);
  });

  test('commit conflict explains the commit window and keeps the cause', () => {
    const cause = new Error('E11000 duplicate key');
    const error = new SessionCommitConflictError('s1', { cause });

    expect(error.sessionId).toBe('s1');
    expect(error.message).toContain("'s1'");
    expect(error.message).toContain('maximum commit duration');
    expect(error.cause).toBe(cause);
  });

  test('closed session reports its state', () => {
    expect(new SessionClosedError('s1', 'committed').message).toContain('already committed');
    expect(new SessionClosedError('s1', 'rolledBack').message).toContain('already rolled back');
  });
});
