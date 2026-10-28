import { randomUUID } from 'node:crypto';
import { runWithSession } from './context.js';
import { createControlMessageHandler, resolveControlTiming, type ControlTimingOptions } from './control-handler.js';
import { FactoryNotStartedError, MissingResourcesError, ZusammenError } from './errors.js';
import { silentLogger } from './logger.js';
import type { PersistenceProvider } from './persistence.js';
import {
  DEFAULT_MAX_COMMIT_DURATION_MS,
  type OpenSessionOptions,
  type SessionFactory,
  type SessionFactoryOptions,
  type TransactionalSession,
} from './session.js';
import { Session, type SessionDependencies, type SessionSettings, type SessionTransaction } from './session-impl.js';
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
  const createResources = options.createResources ?? false;
  const deps: SessionDependencies<TContext> = {
    persistence: options.persistence,
    transport: options.transport,
    convention: options.convention ?? zusammenConvention(),
    logger: options.logger ?? silentLogger,
    now: options.now ?? (() => new Date()),
    newId: options.newId ?? randomUUID,
  };

  let stopControl: StopControlProcessing | undefined;

  const ensureStarted = (): void => {
    if (stopControl === undefined) {
      throw new FactoryNotStartedError();
    }
  };

  const sessionSettings = (openOptions: OpenSessionOptions): SessionSettings => ({
    sessionId: deps.newId(),
    maxCommitDurationMs: validateDuration(openOptions.maxCommitDurationMs ?? maxCommitDurationMs),
    initialCommitDelayIncrementMs: timing.initialCommitDelayIncrementMs,
  });

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
      try {
        await deps.transport.connect();
        if (createResources) {
          await deps.persistence.createResources?.();
          await deps.transport.createResources?.();
        }
        await verifyResources(deps.persistence, deps.transport);
        stopControl = await deps.transport.consumeControl(
          createControlMessageHandler({
            persistence: deps.persistence,
            transport: deps.transport,
            logger: deps.logger,
            timing,
          }),
        );
      } catch (error) {
        await deps.transport.disconnect().catch(() => undefined);
        await deps.persistence.disconnect().catch(() => undefined);
        throw error;
      }
    },

    stop,

    async open(openOptions: OpenSessionOptions = {}): Promise<TransactionalSession<TContext>> {
      ensureStarted();
      const transactions = providerTransactions(deps.persistence);
      const settings = sessionSettings(openOptions);
      const context = await transactions.begin();
      const transaction: SessionTransaction = {
        commit: () => transactions.commit(context),
        rollback: () => transactions.rollback(context),
      };
      return new Session(deps, settings, context, transaction);
    },

    async runInTransaction<T>(
      runTransaction: (callback: (context: TContext) => Promise<T>) => Promise<T>,
      work: (session: TransactionalSession<TContext>) => Promise<T>,
      openOptions: OpenSessionOptions = {},
    ): Promise<T> {
      ensureStarted();
      sessionSettings(openOptions); // validates the options before the transaction starts
      let session = undefined as Session<TContext> | undefined;
      let result: T;
      try {
        result = await runTransaction(async (context) => {
          // A new session per call: transaction APIs that retry their callback start over
          session?.rolledBack();
          const current = new Session(deps, sessionSettings(openOptions), context, undefined);
          session = current;
          const value = await runWithSession(current, () => work(current));
          await current.prepare();
          return value;
        });
      } catch (error) {
        session?.rolledBack();
        throw error;
      }
      if (session?.status === 'committing') {
        await session.committed();
      }
      return result;
    },

    async [Symbol.asyncDispose]() {
      if (stopControl !== undefined) {
        await stop();
      }
    },
  };
}

// Reports everything missing at once, not only the first provider's resources
async function verifyResources(...providers: { verifyResources?(): Promise<void> }[]): Promise<void> {
  const missing: string[] = [];
  for (const provider of providers) {
    try {
      await provider.verifyResources?.();
    } catch (error) {
      if (!(error instanceof MissingResourcesError)) throw error;
      missing.push(...error.resources);
    }
  }
  if (missing.length > 0) {
    throw new MissingResourcesError(missing);
  }
}

function providerTransactions<TContext>(persistence: PersistenceProvider<TContext>) {
  if (persistence.begin === undefined || persistence.commit === undefined || persistence.rollback === undefined) {
    throw new ZusammenError(
      "The persistence provider doesn't begin transactions: use runInTransaction() with your transaction API.",
    );
  }
  return {
    begin: persistence.begin.bind(persistence),
    commit: persistence.commit.bind(persistence),
    rollback: persistence.rollback.bind(persistence),
  };
}

function validateDuration(value: number): number {
  if (!Number.isFinite(value) || value < 0) {
    throw new ZusammenError(`maxCommitDurationMs must be a non-negative finite number, got ${String(value)}.`);
  }
  return value;
}
