import type { ApplicationTransactions } from '../persistence.js';

/** A Prisma client, or an extended one: what `$transaction(fn)` passes to `fn` is the transaction client. */
export interface PrismaClientLike<TTransaction> {
  $transaction<R>(fn: (prisma: TTransaction) => Promise<R>): Promise<R>;
}

interface PrismaExecutor {
  $executeRawUnsafe(query: string, ...values: unknown[]): PromiseLike<number>;
}

/** The transaction client Prisma's `prisma.$transaction(fn)` passes to `fn`. */
export type PrismaTransaction<TClient> = TClient extends PrismaClientLike<infer TTransaction> ? TTransaction : never;

/**
 * Runs the outbox insert in Prisma interactive transactions, for `PostgreSQLOrmPersistence`. `prisma` only gives the
 * transaction type: `new PostgreSQLOrmPersistence({ pool, transactions: prismaTransactions(prisma) })`.
 */
export function prismaTransactions<TTransaction extends PrismaExecutor>(
  prisma: PrismaClientLike<TTransaction>,
): ApplicationTransactions<TTransaction>;
export function prismaTransactions(): ApplicationTransactions<PrismaExecutor> {
  return {
    execute: async (tx, sql, parameters) => tx.$executeRawUnsafe(sql, ...parameters),
    sqlState,
  };
}

// Prisma's driver adapters don't keep the pg error: the SQLSTATE is in meta.driverAdapterError.cause.originalCode
function sqlState(error: unknown): string | undefined {
  const code = (error as { meta?: { driverAdapterError?: { cause?: { originalCode?: unknown } } } } | null)?.meta
    ?.driverAdapterError?.cause?.originalCode;
  return typeof code === 'string' ? code : undefined;
}
