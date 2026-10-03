import type { MessageConvention, PublishOptions, SendOptions } from './convention.js';
import type { Logger } from './logger.js';
import type { PersistenceProvider } from './persistence.js';
import type { TransportProvider } from './transport.js';

export const DEFAULT_MAX_COMMIT_DURATION_MS = 15_000;

export type SessionStatus = 'open' | 'committing' | 'committed' | 'rolledBack';

export interface TransactionalSession<TContext> extends AsyncDisposable {
  readonly sessionId: string;
  readonly status: SessionStatus;
  /** Pass to your own data operations so they join the session's transaction (e.g. a MongoDB `ClientSession`). */
  readonly transactionContext: TContext;

  /** Runs the convention now and buffers the message; nothing leaves the process before `commit()`. */
  send(destination: string, message: unknown, options?: SendOptions): Promise<void>;
  /** Runs the convention now and buffers the message; nothing leaves the process before `commit()`. */
  publish(message: unknown, options?: PublishOptions): Promise<void>;

  /** Commits business data and outgoing messages atomically, then dispatches. Throws {@link SessionCommitConflictError} if the commit window was exceeded. */
  commit(): Promise<void>;
  rollback(): Promise<void>;
  /** Rolls back if neither committed nor rolled back. */
  [Symbol.asyncDispose](): Promise<void>;
}

export interface OpenSessionOptions {
  maxCommitDurationMs?: number | undefined;
}

export interface SessionFactoryOptions<TContext> {
  persistence: PersistenceProvider<TContext>;
  transport: TransportProvider;
  /** Defaults to the Zusammen convention. */
  convention?: MessageConvention | undefined;
  /**
   * Whether `start()` creates the resources providers need. Defaults to false: resources are expected to exist (e.g.
   * created by a deployment script calling the providers' `createResources()`) and are verified at startup.
   */
  createResources?: boolean | undefined;
  /** Defaults to {@link DEFAULT_MAX_COMMIT_DURATION_MS}. */
  maxCommitDurationMs?: number | undefined;
  logger?: Logger | undefined;
}

export interface SessionFactory<TContext> extends AsyncDisposable {
  /** Connects the providers, creates (if enabled) and verifies resources, and starts control message processing. */
  start(): Promise<void>;
  /** Stops control message processing and disconnects the providers. */
  stop(): Promise<void>;
  /** Throws {@link FactoryNotStartedError} before `start()`. */
  open(options?: OpenSessionOptions): Promise<TransactionalSession<TContext>>;
}
