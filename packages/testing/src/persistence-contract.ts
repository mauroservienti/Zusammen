import { describe, expect, test } from 'vitest';
import {
  SessionCommitConflictError,
  type OutboxRecord,
  type PersistenceProvider,
  type TransportOperation,
} from '@zusammen/core';

/** A provider that begins, commits and rolls back transactions, as sessions opened with `factory.open()` need. */
export type TransactionalPersistenceProvider<TContext> = PersistenceProvider<TContext> &
  Required<Pick<PersistenceProvider<TContext>, 'begin' | 'commit' | 'rollback'>>;

export interface PersistenceContractFixture<TContext> {
  persistence: TransactionalPersistenceProvider<TContext>;
  /** Writes business data within the transaction. */
  write(context: TContext, key: string, value: string): Promise<void>;
  /** Reads committed business data. */
  read(key: string): Promise<string | undefined>;
}

export interface PersistenceContractOptions<TContext> {
  /** Creates a connected provider with empty storage; called once per test. */
  setup(): Promise<PersistenceContractFixture<TContext>>;
}

let sequence = 0;
const uniqueId = (prefix: string) => `${prefix}-${String(Date.now())}-${String(++sequence)}`;

export function sampleRecord(id = uniqueId('session')): OutboxRecord {
  const send: TransportOperation = {
    messageId: uniqueId('message'),
    intent: 'send',
    destination: 'billing',
    messageType: 'Sales.PlaceOrder',
    headers: { 'zusammen.message-id': 'm1', 'with.dots': 'and $dollars', empty: '' },
    body: new Uint8Array([0, 1, 2, 250, 255]),
    properties: { contentType: 'application/json', correlationId: 'c1', replyTo: 'sales' },
  };
  const publish: TransportOperation = {
    messageId: uniqueId('message'),
    intent: 'publish',
    topic: 'sales.orders',
    messageType: 'Sales.OrderPlaced',
    headers: {},
    body: new TextEncoder().encode('{"orderId":"o1"}'),
    properties: { contentType: 'application/json' },
  };
  return { id, dispatched: false, transportOperations: [send, publish] };
}

/** Behavior every {@link PersistenceProvider} must provide for transactional sessions to be safe. */
export function persistenceProviderContract<TContext>(name: string, options: PersistenceContractOptions<TContext>) {
  describe(`${name}: persistence provider contract`, () => {
    async function commitRecord(fixture: PersistenceContractFixture<TContext>, record = sampleRecord()) {
      const context = await fixture.persistence.begin();
      await fixture.persistence.storeOutbox(record, context);
      await fixture.persistence.commit(context);
      return record;
    }

    test('a committed record round-trips exactly', async () => {
      const fixture = await options.setup();
      const record = await commitRecord(fixture);

      const stored = await fixture.persistence.get(record.id);

      expect(stored).toEqual(record);
      expect(stored?.transportOperations[0]?.body).toBeInstanceOf(Uint8Array);
    });

    test('unknown sessions have no record', async () => {
      const fixture = await options.setup();
      expect(await fixture.persistence.get(uniqueId('unknown'))).toBeNull();
    });

    test('business data and the outbox record commit together', async () => {
      const fixture = await options.setup();
      const key = uniqueId('order');
      const record = sampleRecord();

      const context = await fixture.persistence.begin();
      await fixture.write(context, key, 'placed');
      await fixture.persistence.storeOutbox(record, context);
      expect(await fixture.persistence.get(record.id)).toBeNull();
      expect(await fixture.read(key)).toBeUndefined();
      await fixture.persistence.commit(context);

      expect(await fixture.read(key)).toBe('placed');
      expect(await fixture.persistence.get(record.id)).not.toBeNull();
    });

    test('rollback discards business data and the outbox record', async () => {
      const fixture = await options.setup();
      const key = uniqueId('order');
      const record = sampleRecord();

      const context = await fixture.persistence.begin();
      await fixture.write(context, key, 'placed');
      await fixture.persistence.storeOutbox(record, context);
      await fixture.persistence.rollback(context);

      expect(await fixture.read(key)).toBeUndefined();
      expect(await fixture.persistence.get(record.id)).toBeNull();
    });

    test('markDispatched flags the record and records when', async () => {
      const fixture = await options.setup();
      const record = await commitRecord(fixture);
      const before = Date.now();

      await fixture.persistence.markDispatched(record.id);
      await fixture.persistence.markDispatched(record.id); // idempotent

      const stored = await fixture.persistence.get(record.id);
      expect(stored?.dispatched).toBe(true);
      expect(stored?.dispatchedAt?.getTime()).toBeGreaterThanOrEqual(before - 1_000);
      expect(stored?.transportOperations).toEqual(record.transportOperations);
    });

    test('a tombstone is a dispatched, empty record; storing it twice reports exists', async () => {
      const fixture = await options.setup();
      const id = uniqueId('session');

      expect(await fixture.persistence.storeTombstone(id)).toBe('stored');
      expect(await fixture.persistence.storeTombstone(id)).toBe('exists');
      expect(await fixture.persistence.get(id)).toMatchObject({ id, dispatched: true, transportOperations: [] });
    });

    test('a tombstone does not overwrite a committed record', async () => {
      const fixture = await options.setup();
      const record = await commitRecord(fixture);

      expect(await fixture.persistence.storeTombstone(record.id)).toBe('exists');
      expect(await fixture.persistence.get(record.id)).toEqual(record);
    });

    test('a session started before the tombstone cannot commit, and its business data is discarded', async () => {
      const fixture = await options.setup();
      const key = uniqueId('order');
      const record = sampleRecord();

      const context = await fixture.persistence.begin();
      await fixture.write(context, key, 'placed');
      expect(await fixture.persistence.storeTombstone(record.id)).toBe('stored');

      const commit = async () => {
        await fixture.persistence.storeOutbox(record, context);
        await fixture.persistence.commit(context);
      };
      await expect(commit()).rejects.toBeInstanceOf(SessionCommitConflictError);
      await fixture.persistence.rollback(context).catch(() => undefined);

      expect(await fixture.read(key)).toBeUndefined();
      expect(await fixture.persistence.get(record.id)).toMatchObject({ dispatched: true, transportOperations: [] });
    });

    test('a tombstone racing a commit: exactly one of them wins', async () => {
      const fixture = await options.setup();
      const record = sampleRecord();

      const context = await fixture.persistence.begin();
      await fixture.persistence.storeOutbox(record, context);
      const [tombstone, commit] = await Promise.allSettled([
        fixture.persistence.storeTombstone(record.id),
        fixture.persistence.commit(context),
      ]);

      const stored = await fixture.persistence.get(record.id);
      if (commit.status === 'fulfilled') {
        expect(tombstone).toEqual({ status: 'fulfilled', value: 'exists' });
        expect(stored).toEqual(record);
      } else {
        expect(commit.reason).toBeInstanceOf(SessionCommitConflictError);
        expect(tombstone).toEqual({ status: 'fulfilled', value: 'stored' });
        expect(stored).toMatchObject({ dispatched: true, transportOperations: [] });
      }
    });

    test('sessions are independent', async () => {
      const fixture = await options.setup();
      const committed = sampleRecord();
      const rolledBack = sampleRecord();

      const first = await fixture.persistence.begin();
      const second = await fixture.persistence.begin();
      await fixture.persistence.storeOutbox(committed, first);
      await fixture.persistence.storeOutbox(rolledBack, second);
      await fixture.persistence.commit(first);
      await fixture.persistence.rollback(second);

      expect(await fixture.persistence.get(committed.id)).toEqual(committed);
      expect(await fixture.persistence.get(rolledBack.id)).toBeNull();
    });
  });
}

export interface ApplicationTransactionFixture<TContext> {
  persistence: PersistenceProvider<TContext>;
  /** Runs `work` in a transaction the application owns (e.g. an ORM's), committing when it returns. */
  runTransaction<T>(work: (context: TContext) => Promise<T>): Promise<T>;
  /** Writes business data within the transaction. */
  write(context: TContext, key: string, value: string): Promise<void>;
  /** Reads committed business data. */
  read(key: string): Promise<string | undefined>;
}

export interface ApplicationTransactionContractOptions<TContext> {
  /** Creates a connected provider with empty storage; called once per test. */
  setup(): Promise<ApplicationTransactionFixture<TContext>>;
}

/** Behavior a {@link PersistenceProvider} must provide for sessions in transactions the application runs. */
export function applicationTransactionContract<TContext>(
  name: string,
  options: ApplicationTransactionContractOptions<TContext>,
) {
  describe(`${name}: application transaction contract`, () => {
    test('business data and the outbox record commit with the application transaction', async () => {
      const fixture = await options.setup();
      const key = uniqueId('order');
      const record = sampleRecord();

      await fixture.runTransaction(async (context) => {
        await fixture.write(context, key, 'placed');
        await fixture.persistence.storeOutbox(record, context);
        expect(await fixture.persistence.get(record.id)).toBeNull();
      });

      expect(await fixture.read(key)).toBe('placed');
      expect(await fixture.persistence.get(record.id)).toEqual(record);
    });

    test('a failing application transaction discards business data and the outbox record', async () => {
      const fixture = await options.setup();
      const key = uniqueId('order');
      const record = sampleRecord();

      const run = fixture.runTransaction(async (context) => {
        await fixture.write(context, key, 'placed');
        await fixture.persistence.storeOutbox(record, context);
        throw new Error('out of stock');
      });

      await expect(run).rejects.toThrow('out of stock');
      expect(await fixture.read(key)).toBeUndefined();
      expect(await fixture.persistence.get(record.id)).toBeNull();
    });

    test('a tombstone stored first fails the outbox insert, and the business data is discarded', async () => {
      const fixture = await options.setup();
      const key = uniqueId('order');
      const record = sampleRecord();

      const run = fixture.runTransaction(async (context) => {
        await fixture.write(context, key, 'placed');
        expect(await fixture.persistence.storeTombstone(record.id)).toBe('stored');
        await fixture.persistence.storeOutbox(record, context);
      });

      await expect(run).rejects.toBeInstanceOf(SessionCommitConflictError);
      expect(await fixture.read(key)).toBeUndefined();
      expect(await fixture.persistence.get(record.id)).toMatchObject({ dispatched: true, transportOperations: [] });
    });

    test('a tombstone racing the application commit: exactly one of them wins', async () => {
      const fixture = await options.setup();
      const key = uniqueId('order');
      const record = sampleRecord();

      let tombstone: Promise<'stored' | 'exists'> | undefined;
      const [commit] = await Promise.allSettled([
        fixture.runTransaction(async (context) => {
          await fixture.write(context, key, 'placed');
          await fixture.persistence.storeOutbox(record, context);
          tombstone = fixture.persistence.storeTombstone(record.id);
          // Give the tombstone time to reach the store before committing
          await new Promise((resolve) => setTimeout(resolve, 200));
        }),
      ]);

      const stored = await fixture.persistence.get(record.id);
      if (commit.status === 'fulfilled') {
        expect(await tombstone).toBe('exists');
        expect(stored).toEqual(record);
        expect(await fixture.read(key)).toBe('placed');
      } else {
        expect(commit.reason).toBeInstanceOf(SessionCommitConflictError);
        expect(await tombstone).toBe('stored');
        expect(stored).toMatchObject({ dispatched: true, transportOperations: [] });
        expect(await fixture.read(key)).toBeUndefined();
      }
    });
  });
}
