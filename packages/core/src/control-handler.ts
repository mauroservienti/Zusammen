import type { ControlMessage, ControlMessageHandler, ControlResult } from './control.js';
import type { Logger } from './logger.js';
import type { OutboxRecord } from './outbox.js';
import type { PersistenceProvider } from './persistence.js';
import type { TransportProvider } from './transport.js';

export interface ControlTimingOptions {
  /** First wait for a pending commit; doubles on every retry. Default 1 s. */
  initialCommitDelayIncrementMs?: number | undefined;
  /** Upper bound for the wait for a pending commit. Default 10 s. */
  maxCommitDelayIncrementMs?: number | undefined;
  /** Failures (e.g. dispatch errors) tolerated before giving up. Default 10. */
  maxFailures?: number | undefined;
  /** First delay after a failure; doubles on every failure. Default 1 s. */
  initialFailureDelayMs?: number | undefined;
  /** Upper bound for the delay after a failure. Default 60 s. */
  maxFailureDelayMs?: number | undefined;
}

export type ResolvedControlTiming = Required<{ [K in keyof ControlTimingOptions]: number }>;

export function resolveControlTiming(options: ControlTimingOptions = {}): ResolvedControlTiming {
  return {
    initialCommitDelayIncrementMs: options.initialCommitDelayIncrementMs ?? 1_000,
    maxCommitDelayIncrementMs: options.maxCommitDelayIncrementMs ?? 10_000,
    maxFailures: options.maxFailures ?? 10,
    initialFailureDelayMs: options.initialFailureDelayMs ?? 1_000,
    maxFailureDelayMs: options.maxFailureDelayMs ?? 60_000,
  };
}

export interface ControlMessageHandlerOptions<TContext> {
  persistence: PersistenceProvider<TContext>;
  transport: TransportProvider;
  logger: Logger;
  timing?: ControlTimingOptions | undefined;
}

/**
 * Transport-agnostic decision logic for control messages:
 *
 * - record dispatched (or tombstone) → ack
 * - record not dispatched → dispatch, mark dispatched, ack
 * - no record, commit window open → retry after a growing delay
 * - no record, commit window expired → store a tombstone so a late commit fails, ack
 *
 * Errors are retried with backoff until `maxFailures`, then reported as `error` for the transport to dead-letter.
 */
export function createControlMessageHandler<TContext>(
  options: ControlMessageHandlerOptions<TContext>,
): ControlMessageHandler {
  const { persistence, transport, logger } = options;
  const timing = resolveControlTiming(options.timing);

  const dispatch = async (record: OutboxRecord): Promise<ControlResult> => {
    if (!record.dispatched) {
      await transport.dispatch(record.transportOperations);
      await persistence.markDispatched(record.id);
      logger.debug('Dispatched outbox record from control message', {
        sessionId: record.id,
        operations: record.transportOperations.length,
      });
    }
    return { kind: 'ack' };
  };

  const process = async (message: ControlMessage): Promise<ControlResult> => {
    const record = await persistence.get(message.sessionId);
    if (record !== null) {
      return dispatch(record);
    }

    if (message.remainingCommitDurationMs > 0) {
      const delayMs = Math.min(message.commitDelayIncrementMs, message.remainingCommitDurationMs);
      return {
        kind: 'retry',
        delayMs,
        next: {
          ...message,
          attempt: message.attempt + 1,
          remainingCommitDurationMs: message.remainingCommitDurationMs - delayMs,
          commitDelayIncrementMs: Math.min(message.commitDelayIncrementMs * 2, timing.maxCommitDelayIncrementMs),
        },
      };
    }

    if ((await persistence.storeTombstone(message.sessionId)) === 'stored') {
      logger.warn('Commit window expired without a committed outbox record, stored a tombstone', {
        sessionId: message.sessionId,
      });
      return { kind: 'ack' };
    }

    // The commit landed while storing the tombstone
    const committed = await persistence.get(message.sessionId);
    return committed === null ? { kind: 'ack' } : dispatch(committed);
  };

  return async (message) => {
    try {
      return await process(message);
    } catch (caught) {
      const error = caught instanceof Error ? caught : new Error(String(caught));
      const failures = message.failures + 1;
      if (failures >= timing.maxFailures) {
        logger.error('Giving up on control message', { sessionId: message.sessionId, failures, error });
        return { kind: 'error', error };
      }
      const delayMs = Math.min(timing.initialFailureDelayMs * 2 ** (failures - 1), timing.maxFailureDelayMs);
      logger.warn('Control message processing failed, retrying', {
        sessionId: message.sessionId,
        failures,
        delayMs,
        error,
      });
      return { kind: 'retry', delayMs, next: { ...message, attempt: message.attempt + 1, failures } };
    }
  };
}
