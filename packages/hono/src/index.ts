import type { Context, MiddlewareHandler } from 'hono';
import {
  runWithSession,
  settleSession,
  type OpenSessionOptions,
  type SessionFactory,
  type TransactionalSession,
} from '@zusammen/core';

export interface TransactionalSessionVariables {
  transactionalSession: TransactionalSession<unknown>;
}

export interface TransactionalSessionOptions {
  /** Options for every session the middleware opens. */
  session?: OpenSessionOptions | undefined;
  /** Whether to commit, decided before the response is returned. Defaults to commit for status codes below 400. */
  shouldCommit?: ((context: Context) => boolean) | undefined;
  /** The response when the commit fails. Defaults to an empty 500 response. */
  onCommitError?: ((error: unknown, context: Context) => Response) | undefined;
}

/**
 * Opens a transactional session per request and commits it (or rolls it back) before the response is returned:
 * a client never sees success for a commit that failed.
 */
export function transactionalSession<TContext>(
  factory: SessionFactory<TContext>,
  options: TransactionalSessionOptions = {},
): MiddlewareHandler<{ Variables: TransactionalSessionVariables }> {
  const shouldCommit = options.shouldCommit ?? ((context: Context) => context.res.status < 400);
  const onCommitError = options.onCommitError ?? (() => new Response(null, { status: 500 }));

  return async (context, next) => {
    const session = await factory.open(options.session);
    context.set('transactionalSession', session);
    try {
      await runWithSession(session, next);
      await settleSession(session, context.error === undefined && shouldCommit(context));
    } catch (error) {
      await session[Symbol.asyncDispose]();
      // Replace, not merge: the handler's headers describe a response that didn't happen
      context.res = undefined;
      context.res = onCommitError(error, context);
    }
  };
}

/** The request's session. */
export function sessionOf<TContext = unknown>(
  context: Context<{ Variables: TransactionalSessionVariables }>,
): TransactionalSession<TContext> {
  return context.get('transactionalSession') as TransactionalSession<TContext>;
}
