import type { ControlMessage } from './control.js';
import type {
  ConventionContext,
  MessageConvention,
  OutgoingMessage,
  PublishOptions,
  SendOptions,
} from './convention.js';
import { ApplicationTransactionError, SessionClosedError } from './errors.js';
import type { Logger } from './logger.js';
import type { TransportOperation } from './outbox.js';
import type { PersistenceProvider } from './persistence.js';
import type { SessionStatus, TransactionalSession } from './session.js';
import type { TransportProvider } from './transport.js';

export interface SessionDependencies<TContext> {
  persistence: PersistenceProvider<TContext>;
  transport: TransportProvider;
  convention: MessageConvention;
  logger: Logger;
  now: () => Date;
  newId: () => string;
}

export interface SessionSettings {
  sessionId: string;
  maxCommitDurationMs: number;
  initialCommitDelayIncrementMs: number;
}

/** Commits or rolls back a session's transaction. Absent when the application runs the transaction (`runInTransaction`). */
export interface SessionTransaction {
  commit(): Promise<void>;
  rollback(): Promise<void>;
}

export class Session<TContext> implements TransactionalSession<TContext> {
  readonly #deps: SessionDependencies<TContext>;
  readonly #settings: SessionSettings;
  readonly #transaction: SessionTransaction | undefined;
  readonly #operations: TransportOperation[] = [];
  readonly #conventionContext: ConventionContext;
  #state: SessionStatus = 'open';

  constructor(
    deps: SessionDependencies<TContext>,
    settings: SessionSettings,
    readonly transactionContext: TContext,
    transaction: SessionTransaction | undefined,
  ) {
    this.#deps = deps;
    this.#settings = settings;
    this.#transaction = transaction;
    this.#conventionContext = { sessionId: settings.sessionId, now: deps.now, newId: deps.newId };
  }

  get sessionId(): string {
    return this.#settings.sessionId;
  }

  get status(): SessionStatus {
    return this.#state;
  }

  send(destination: string, message: unknown, options: SendOptions = {}): Promise<void> {
    return this.#add({ intent: 'send', destination, message, options });
  }

  publish(message: unknown, options: PublishOptions = {}): Promise<void> {
    return this.#add({ intent: 'publish', message, options });
  }

  async commit(): Promise<void> {
    const transaction = this.#ownTransaction();
    this.#ensureOpen();
    this.#state = 'committing';
    await this.#finish(async () => {
      await this.#store();
      await transaction.commit();
    });
    await this.#dispatch();
  }

  async rollback(): Promise<void> {
    const transaction = this.#ownTransaction();
    this.#ensureOpen();
    this.#state = 'rolledBack';
    await transaction.rollback();
  }

  async [Symbol.asyncDispose](): Promise<void> {
    // The application rolls back its own transaction by throwing from the runInTransaction callback
    if (this.#state === 'open' && this.#transaction !== undefined) {
      await this.rollback();
    }
  }

  /** For `runInTransaction`: stores the outbox record in the application's transaction, which it then commits. */
  async prepare(): Promise<void> {
    this.#ensureOpen();
    this.#state = 'committing';
    try {
      await this.#store();
    } catch (error) {
      this.#state = 'rolledBack';
      throw error;
    }
  }

  /** For `runInTransaction`: the application's transaction committed. */
  async committed(): Promise<void> {
    this.#state = 'committed';
    await this.#dispatch();
  }

  /** For `runInTransaction`: the application's transaction didn't commit; a stored control message tombstones it. */
  rolledBack(): void {
    this.#state = 'rolledBack';
  }

  // Control message first: once the transaction commits, dispatch is guaranteed even if this process dies. Delayed, so
  // that it normally finds the immediate dispatch done; the delay doesn't count against the window.
  async #store(): Promise<void> {
    if (this.#operations.length === 0) {
      return;
    }
    const control: ControlMessage = {
      sessionId: this.sessionId,
      remainingCommitDurationMs: this.#settings.maxCommitDurationMs,
      commitDelayIncrementMs: this.#settings.initialCommitDelayIncrementMs,
      attempt: 1,
      failures: 0,
    };
    await this.#deps.transport.sendControl(control, this.#settings.initialCommitDelayIncrementMs);
    await this.#deps.persistence.storeOutbox(
      { id: this.sessionId, dispatched: false, transportOperations: this.#operations },
      this.transactionContext,
    );
  }

  async #dispatch(): Promise<void> {
    if (this.#operations.length === 0) {
      return;
    }
    try {
      await this.#deps.transport.dispatch(this.#operations);
      await this.#deps.persistence.markDispatched(this.sessionId);
    } catch (error) {
      this.#deps.logger.warn('Immediate dispatch failed, the control message will dispatch', {
        sessionId: this.sessionId,
        error,
      });
    }
  }

  // Runs the commit steps; on failure rolls back and rethrows, leaving the session rolled back
  async #finish(steps: () => Promise<void>): Promise<void> {
    try {
      await steps();
      this.#state = 'committed';
    } catch (error) {
      this.#state = 'rolledBack';
      try {
        await this.#ownTransaction().rollback();
      } catch (rollbackError) {
        this.#deps.logger.debug('Rollback after failed commit failed', {
          sessionId: this.sessionId,
          error: rollbackError,
        });
      }
      throw error;
    }
  }

  #ownTransaction(): SessionTransaction {
    if (this.#transaction === undefined) {
      throw new ApplicationTransactionError(this.sessionId);
    }
    return this.#transaction;
  }

  // async so that validation and convention errors surface as rejections, like every other session failure
  // eslint-disable-next-line @typescript-eslint/require-await
  async #add(input: OutgoingMessage): Promise<void> {
    this.#ensureOpen();
    this.#operations.push(this.#deps.convention.toOperation(input, this.#conventionContext));
  }

  #ensureOpen(): void {
    if (this.#state !== 'open') {
      throw new SessionClosedError(this.sessionId, this.#state);
    }
  }
}
