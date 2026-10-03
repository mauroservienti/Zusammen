import type { ControlMessage } from './control.js';
import type {
  ConventionContext,
  MessageConvention,
  OutgoingMessage,
  PublishOptions,
  SendOptions,
} from './convention.js';
import { SessionClosedError } from './errors.js';
import type { Logger } from './logger.js';
import type { TransportOperation } from './outbox.js';
import type { PersistenceProvider } from './persistence.js';
import type { TransactionalSession } from './session.js';
import type { TransportProvider } from './transport.js';

type SessionState = 'open' | 'committing' | 'committed' | 'rolledBack';

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

export class Session<TContext> implements TransactionalSession<TContext> {
  readonly #deps: SessionDependencies<TContext>;
  readonly #settings: SessionSettings;
  readonly #operations: TransportOperation[] = [];
  readonly #conventionContext: ConventionContext;
  #state: SessionState = 'open';

  constructor(
    deps: SessionDependencies<TContext>,
    settings: SessionSettings,
    readonly transactionContext: TContext,
  ) {
    this.#deps = deps;
    this.#settings = settings;
    this.#conventionContext = { sessionId: settings.sessionId, now: deps.now, newId: deps.newId };
  }

  get sessionId(): string {
    return this.#settings.sessionId;
  }

  send(destination: string, message: unknown, options: SendOptions = {}): Promise<void> {
    return this.#add({ intent: 'send', destination, message, options });
  }

  publish(message: unknown, options: PublishOptions = {}): Promise<void> {
    return this.#add({ intent: 'publish', message, options });
  }

  async commit(): Promise<void> {
    this.#ensureOpen();
    this.#state = 'committing';
    const { persistence, transport, logger } = this.#deps;

    if (this.#operations.length === 0) {
      await this.#finish(() => persistence.commit(this.transactionContext));
      return;
    }

    const control: ControlMessage = {
      sessionId: this.sessionId,
      remainingCommitDurationMs: this.#settings.maxCommitDurationMs,
      commitDelayIncrementMs: this.#settings.initialCommitDelayIncrementMs,
      attempt: 1,
      failures: 0,
    };

    await this.#finish(async () => {
      // Control message first: once the transaction commits, dispatch is guaranteed even if this process dies
      await transport.sendControl(control);
      await persistence.storeOutbox(
        { id: this.sessionId, dispatched: false, transportOperations: this.#operations },
        this.transactionContext,
      );
      await persistence.commit(this.transactionContext);
    });

    try {
      await transport.dispatch(this.#operations);
      await persistence.markDispatched(this.sessionId);
    } catch (error) {
      logger.warn('Immediate dispatch failed, the control message will dispatch', { sessionId: this.sessionId, error });
    }
  }

  async rollback(): Promise<void> {
    this.#ensureOpen();
    this.#state = 'rolledBack';
    await this.#deps.persistence.rollback(this.transactionContext);
  }

  async [Symbol.asyncDispose](): Promise<void> {
    if (this.#state === 'open') {
      await this.rollback();
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
        await this.#deps.persistence.rollback(this.transactionContext);
      } catch (rollbackError) {
        this.#deps.logger.debug('Rollback after failed commit failed', {
          sessionId: this.sessionId,
          error: rollbackError,
        });
      }
      throw error;
    }
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
