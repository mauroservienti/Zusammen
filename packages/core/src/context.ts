import { AsyncLocalStorage } from 'node:async_hooks';
import { ZusammenError } from './errors.js';
import type { OpenSessionOptions, SessionFactory, TransactionalSession } from './session.js';

const storage = new AsyncLocalStorage<TransactionalSession<unknown>>();

export class NoActiveSessionError extends ZusammenError {
  constructor() {
    super('No transactional session is active. Use withSession() or a framework adapter to open one.');
  }
}

/** The session of the current async context (request), as opened by {@link withSession} or a framework adapter. */
export function getSession<TContext = unknown>(): TransactionalSession<TContext> {
  const session = storage.getStore();
  if (session === undefined) {
    throw new NoActiveSessionError();
  }
  return session as TransactionalSession<TContext>;
}

/** Like {@link getSession}, but returns undefined when no session is active. */
export function tryGetSession<TContext = unknown>(): TransactionalSession<TContext> | undefined {
  return storage.getStore() as TransactionalSession<TContext> | undefined;
}

/** Runs `fn` with `session` as the current session; for framework adapters. */
export function runWithSession<T>(session: TransactionalSession<unknown>, fn: () => T): T {
  return storage.run(session, fn);
}

/**
 * Opens a session, runs `fn` with it as the current session, and commits when `fn` succeeds or rolls back when it
 * throws. `fn` may commit or roll back itself; the session is then left as is.
 */
export async function withSession<TContext, T>(
  factory: SessionFactory<TContext>,
  fn: (session: TransactionalSession<TContext>) => Promise<T>,
  options?: OpenSessionOptions,
): Promise<T> {
  const session = await factory.open(options);
  try {
    const result = await runWithSession(session, () => fn(session));
    if (session.status === 'open') {
      await session.commit();
    }
    return result;
  } finally {
    await session[Symbol.asyncDispose]();
  }
}

/**
 * Ends a session opened by a framework adapter: commits on success, rolls back otherwise. Sessions the application
 * already committed or rolled back are left as is.
 */
export async function settleSession(session: TransactionalSession<unknown>, succeeded: boolean): Promise<void> {
  if (session.status !== 'open') {
    return;
  }
  if (succeeded) {
    await session.commit();
  } else {
    await session.rollback();
  }
}
