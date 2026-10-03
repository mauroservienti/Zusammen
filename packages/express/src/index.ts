import type { NextFunction, Request, RequestHandler, Response } from 'express';
import {
  runWithSession,
  settleSession,
  type OpenSessionOptions,
  type SessionFactory,
  type TransactionalSession,
} from '@zusammen/core';

// Express's Request extends the global Express.Request, the documented extension point
declare global {
  // eslint-disable-next-line @typescript-eslint/no-namespace -- augmenting Express's global namespace
  namespace Express {
    interface Request {
      /** Set by the Zusammen middleware. */
      transactionalSession?: TransactionalSession<unknown>;
    }
  }
}

export interface TransactionalSessionOptions {
  /** Options for every session the middleware opens. */
  session?: OpenSessionOptions | undefined;
  /** Whether to commit, decided when the response starts. Defaults to commit for status codes below 400. */
  shouldCommit?: ((response: Response) => boolean) | undefined;
  /** Responds when the commit fails, before anything was sent. Defaults to an empty 500 response. */
  onCommitError?: ((error: unknown, request: Request, response: Response) => void) | undefined;
}

const defaultShouldCommit = (response: Response) => response.statusCode < 400;

const defaultOnCommitError = (_error: unknown, _request: Request, response: Response) => {
  response.statusCode = 500;
  response.end();
};

type Intercepted = 'writeHead' | 'write' | 'end' | 'flushHeaders';
const INTERCEPTED: readonly Intercepted[] = ['writeHead', 'write', 'end', 'flushHeaders'];

/**
 * Opens a transactional session per request. The session is committed (or rolled back) when the response starts,
 * before anything reaches the client: a client never sees success for a commit that failed.
 */
export function transactionalSession<TContext>(
  factory: SessionFactory<TContext>,
  options: TransactionalSessionOptions = {},
): RequestHandler {
  const shouldCommit = options.shouldCommit ?? defaultShouldCommit;
  const onCommitError = options.onCommitError ?? defaultOnCommitError;

  return (request: Request, response: Response, next: NextFunction) => {
    factory.open(options.session).then((session) => {
      request.transactionalSession = session;
      holdResponseUntilSettled(request, response, session, shouldCommit, onCommitError);
      response.once('close', () => {
        // Client went away before a response started
        if (session.status === 'open') {
          session.rollback().catch(() => undefined);
        }
      });
      runWithSession(session, () => {
        next();
      });
    }, next);
  };
}

/** The request's session; throws if the middleware didn't run. */
export function sessionOf<TContext = unknown>(request: Request): TransactionalSession<TContext> {
  if (request.transactionalSession === undefined) {
    throw new Error('No transactional session on this request; is the Zusammen middleware installed for this route?');
  }
  return request.transactionalSession as TransactionalSession<TContext>;
}

// Buffers the first response calls (headers aren't sent yet), settles the session, then replays them
function holdResponseUntilSettled(
  request: Request,
  response: Response,
  session: TransactionalSession<unknown>,
  shouldCommit: (response: Response) => boolean,
  onCommitError: (error: unknown, request: Request, response: Response) => void,
) {
  /* eslint-disable @typescript-eslint/no-unsafe-function-type, @typescript-eslint/no-unsafe-return -- patching Node response methods generically */
  const target = response as unknown as Record<Intercepted, Function>;
  const originals = Object.fromEntries(INTERCEPTED.map((name) => [name, target[name]])) as Record<
    Intercepted,
    Function
  >;
  const pending: (() => void)[] = [];
  let settling = false;

  const restore = () => {
    for (const name of INTERCEPTED) target[name] = originals[name];
  };

  const settle = async () => {
    try {
      await settleSession(session, shouldCommit(response));
    } catch (error) {
      restore();
      for (const name of response.getHeaderNames()) response.removeHeader(name);
      onCommitError(error, request, response);
      return;
    }
    restore();
    for (const call of pending) call();
  };

  for (const name of INTERCEPTED) {
    target[name] = function (this: unknown, ...args: unknown[]) {
      pending.push(() => originals[name].apply(response, args));
      if (!settling) {
        settling = true;
        void settle();
      }
      return name === 'write' ? true : response;
    };
  }
  /* eslint-enable */
}
