import {
  createParamDecorator,
  Global,
  Inject,
  Injectable,
  Module,
  SetMetadata,
  type CallHandler,
  type DynamicModule,
  type ExecutionContext,
  type InjectionToken,
  type ModuleMetadata,
  type NestInterceptor,
  type OnApplicationShutdown,
  type OnModuleInit,
  type OptionalFactoryDependency,
} from '@nestjs/common';
import { APP_INTERCEPTOR, Reflector } from '@nestjs/core';
import { defer, lastValueFrom, type Observable } from 'rxjs';
import {
  getSession,
  runWithSession,
  type OpenSessionOptions,
  type SessionFactory,
  type TransactionalSession,
} from '@zusammen/core';

export const ZUSAMMEN_SESSION_FACTORY = Symbol('ZUSAMMEN_SESSION_FACTORY');
const ZUSAMMEN_OPTIONS = Symbol('ZUSAMMEN_OPTIONS');
const TRANSACTIONAL = 'zusammen:transactional';

export interface ZusammenModuleOptions {
  /** Opens a session for every route, not only routes marked with {@link Transactional}. */
  global?: boolean | undefined;
  /** Options for every session the interceptor opens. */
  session?: OpenSessionOptions | undefined;
  /** Starts the factory when the module initializes and stops it on shutdown. Defaults to true. */
  manageLifecycle?: boolean | undefined;
}

export interface ZusammenModuleSyncOptions extends ZusammenModuleOptions {
  // eslint-disable-next-line @typescript-eslint/no-explicit-any -- any persistence context
  factory: SessionFactory<any>;
}

export interface ZusammenModuleAsyncOptions extends ZusammenModuleOptions {
  imports?: ModuleMetadata['imports'];
  inject?: (InjectionToken | OptionalFactoryDependency)[];
  // eslint-disable-next-line @typescript-eslint/no-explicit-any -- any persistence context and dependencies
  useFactory: (...dependencies: any[]) => SessionFactory<any> | Promise<SessionFactory<any>>;
}

interface HttpRequestWithSession {
  transactionalSession?: TransactionalSession<unknown>;
}

/** Marks a controller or route handler as running in a transactional session. */
export const Transactional = (): MethodDecorator & ClassDecorator => SetMetadata(TRANSACTIONAL, true);

/** Injects the route's transactional session into a handler parameter. */
export const CurrentSession = createParamDecorator((_data: unknown, context: ExecutionContext) => {
  return context.switchToHttp().getRequest<HttpRequestWithSession>().transactionalSession;
});

/** Opens a session around transactional routes; commits when the handler succeeds, before Nest sends the response. */
@Injectable()
export class TransactionalSessionInterceptor implements NestInterceptor {
  constructor(
    @Inject(ZUSAMMEN_SESSION_FACTORY) private readonly factory: SessionFactory<unknown>,
    @Inject(ZUSAMMEN_OPTIONS) private readonly options: ZusammenModuleOptions,
    @Inject(Reflector) private readonly reflector: Reflector,
  ) {}

  intercept(context: ExecutionContext, next: CallHandler): Observable<unknown> {
    const transactional =
      this.options.global === true ||
      this.reflector.getAllAndOverride<boolean | undefined>(TRANSACTIONAL, [context.getHandler(), context.getClass()]);
    if (context.getType() !== 'http' || transactional !== true) {
      return next.handle();
    }
    return defer(() => this.#run(context, next));
  }

  async #run(context: ExecutionContext, next: CallHandler): Promise<unknown> {
    const session = await this.factory.open(this.options.session);
    context.switchToHttp().getRequest<HttpRequestWithSession>().transactionalSession = session;
    try {
      const result: unknown = await runWithSession(session, () =>
        lastValueFrom(next.handle(), { defaultValue: undefined }),
      );
      if (session.status === 'open') {
        await session.commit();
      }
      return result;
    } finally {
      await session[Symbol.asyncDispose]();
    }
  }
}

/** The current request's session, for services that don't receive it as a parameter. */
@Injectable()
export class SessionAccessor {
  get current(): TransactionalSession<unknown> {
    return getSession();
  }
}

@Injectable()
class FactoryLifecycle implements OnModuleInit, OnApplicationShutdown {
  constructor(
    @Inject(ZUSAMMEN_SESSION_FACTORY) private readonly factory: SessionFactory<unknown>,
    @Inject(ZUSAMMEN_OPTIONS) private readonly options: ZusammenModuleOptions,
  ) {}

  async onModuleInit(): Promise<void> {
    if (this.options.manageLifecycle !== false) await this.factory.start();
  }

  async onApplicationShutdown(): Promise<void> {
    if (this.options.manageLifecycle !== false) await this.factory.stop();
  }
}

@Global()
@Module({})
// eslint-disable-next-line @typescript-eslint/no-extraneous-class -- Nest modules are classes with static factories
export class ZusammenModule {
  static forRoot(options: ZusammenModuleSyncOptions): DynamicModule {
    const { factory, ...moduleOptions } = options;
    return ZusammenModule.build([{ provide: ZUSAMMEN_SESSION_FACTORY, useValue: factory }], moduleOptions);
  }

  static forRootAsync(options: ZusammenModuleAsyncOptions): DynamicModule {
    const { useFactory, inject, imports, ...moduleOptions } = options;
    return {
      ...ZusammenModule.build([{ provide: ZUSAMMEN_SESSION_FACTORY, useFactory, inject: inject ?? [] }], moduleOptions),
      imports: imports ?? [],
    };
  }

  private static build(
    factoryProviders: DynamicModule['providers'] & object,
    options: ZusammenModuleOptions,
  ): DynamicModule {
    return {
      module: ZusammenModule,
      providers: [
        ...factoryProviders,
        { provide: ZUSAMMEN_OPTIONS, useValue: options },
        { provide: APP_INTERCEPTOR, useClass: TransactionalSessionInterceptor },
        SessionAccessor,
        FactoryLifecycle,
      ],
      exports: [ZUSAMMEN_SESSION_FACTORY, SessionAccessor],
    };
  }
}
