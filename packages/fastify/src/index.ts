import type { FastifyPluginCallback, FastifyReply, FastifyRequest } from 'fastify';
import fastifyPlugin from 'fastify-plugin';
import {
  runWithSession,
  settleSession,
  type OpenSessionOptions,
  type SessionFactory,
  type TransactionalSession,
} from '@zusammen/core';

declare module 'fastify' {
  interface FastifyRequest {
    /** Set by the Zusammen plugin on routes with a transactional session. */
    transactionalSession: TransactionalSession<unknown> | undefined;
  }
  interface FastifyContextConfig {
    /** Opens a transactional session for this route. */
    transactionalSession?: boolean;
  }
}

export interface ZusammenFastifyOptions {
  // eslint-disable-next-line @typescript-eslint/no-explicit-any -- any persistence context
  factory: SessionFactory<any>;
  /** Opens a session for every route, not only routes with `config: { transactionalSession: true }`. */
  global?: boolean | undefined;
  /** Options for every session the plugin opens. */
  session?: OpenSessionOptions | undefined;
  /** Whether to commit, decided before the response is sent. Defaults to commit for status codes below 400. */
  shouldCommit?: ((reply: FastifyReply) => boolean) | undefined;
}

const plugin: FastifyPluginCallback<ZusammenFastifyOptions> = (fastify, options, done) => {
  const shouldCommit = options.shouldCommit ?? ((reply: FastifyReply) => reply.statusCode < 400);
  const enabled = (request: FastifyRequest) =>
    options.global === true || request.routeOptions.config.transactionalSession === true;

  fastify.decorateRequest('transactionalSession', undefined);

  fastify.addHook('onRequest', (request, _reply, next) => {
    if (!enabled(request)) {
      next();
      return;
    }
    options.factory.open(options.session).then(
      (session: TransactionalSession<unknown>) => {
        request.transactionalSession = session;
        // Run the rest of the request in the session's async context, for getSession()
        runWithSession(session, () => {
          next();
        });
      },
      (error: unknown) => {
        next(error instanceof Error ? error : new Error(String(error)));
      },
    );
  });

  // Runs before the payload is written: a failed commit turns into an error response
  fastify.addHook('onSend', async (request, reply, payload) => {
    if (request.transactionalSession !== undefined) {
      await settleSession(request.transactionalSession, shouldCommit(reply));
    }
    return payload;
  });

  const rollbackIfOpen = async (request: FastifyRequest) => {
    if (request.transactionalSession?.status === 'open') {
      await request.transactionalSession.rollback();
    }
  };
  fastify.addHook('onResponse', rollbackIfOpen);
  fastify.addHook('onRequestAbort', rollbackIfOpen);

  done();
};

/** Opens a transactional session per request and commits it before the response is sent. */
export const zusammen = fastifyPlugin(plugin, { fastify: '5.x', name: '@zusammen/fastify' });

/** The request's session; throws if the route has none. */
export function sessionOf<TContext = unknown>(request: FastifyRequest): TransactionalSession<TContext> {
  if (request.transactionalSession === undefined) {
    throw new Error('No transactional session on this request; enable it with config: { transactionalSession: true }.');
  }
  return request.transactionalSession as TransactionalSession<TContext>;
}
