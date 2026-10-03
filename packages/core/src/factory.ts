import { randomUUID } from 'node:crypto';
import { createControlMessageHandler, resolveControlTiming, type ControlTimingOptions } from './control-handler.js';
import { FactoryNotStartedError, ZusammenError } from './errors.js';
import { silentLogger } from './logger.js';
import {
  DEFAULT_MAX_COMMIT_DURATION_MS,
  type OpenSessionOptions,
  type SessionFactory,
  type SessionFactoryOptions,
  type TransactionalSession,
} from './session.js';
import { Session, type SessionDependencies } from './session-impl.js';
import type { StopControlProcessing } from './transport.js';
import { zusammenConvention } from './zusammen-convention.js';

export interface CreateSessionFactoryOptions<TContext> extends SessionFactoryOptions<TContext> {
  /** Delays and limits for control message processing. */
  controlTiming?: ControlTimingOptions | undefined;
  /** Clock used for message timestamps. Defaults to the system clock. */
  now?: (() => Date) | undefined;
  /** Generator for session and message IDs. Defaults to random UUIDs. */
  newId?: (() => string) | undefined;
}

export function createSessionFactory<TContext>(
  options: CreateSessionFactoryOptions<TContext>,
): SessionFactory<TContext> {
  const maxCommitDurationMs = validateDuration(options.maxCommitDurationMs ?? DEFAULT_MAX_COMMIT_DURATION_MS);
  const timing = resolveControlTiming(options.controlTiming);
  const deps: SessionDependencies<TContext> = {
    persistence: options.persistence,
    transport: options.transport,
    convention: options.convention ?? zusammenConvention(),
    logger: options.logger ?? silentLogger,
    now: options.now ?? (() => new Date()),
    newId: options.newId ?? randomUUID,
  };

  let stopControl: StopControlProcessing | undefined;

  const stop = async (): Promise<void> => {
    const stopping = stopControl;
    stopControl = undefined;
    await stopping?.();
    await deps.transport.disconnect();
    await deps.persistence.disconnect();
  };

  return {
    async start() {
      if (stopControl !== undefined) {
        return;
      }
      deps.transport.validateConvention?.(deps.convention);
      await deps.persistence.connect();
      await deps.transport.connect();
      stopControl = await deps.transport.consumeControl(
        createControlMessageHandler({
          persistence: deps.persistence,
          transport: deps.transport,
          logger: deps.logger,
          timing,
        }),
      );
    },

    stop,

    async open(openOptions: OpenSessionOptions = {}): Promise<TransactionalSession<TContext>> {
      if (stopControl === undefined) {
        throw new FactoryNotStartedError();
      }
      const transactionContext = await deps.persistence.begin();
      return new Session(
        deps,
        {
          sessionId: deps.newId(),
          maxCommitDurationMs: validateDuration(openOptions.maxCommitDurationMs ?? maxCommitDurationMs),
          initialCommitDelayIncrementMs: timing.initialCommitDelayIncrementMs,
        },
        transactionContext,
      );
    },

    async [Symbol.asyncDispose]() {
      if (stopControl !== undefined) {
        await stop();
      }
    },
  };
}

function validateDuration(value: number): number {
  if (!Number.isFinite(value) || value < 0) {
    throw new ZusammenError(`maxCommitDurationMs must be a non-negative finite number, got ${String(value)}.`);
  }
  return value;
}
