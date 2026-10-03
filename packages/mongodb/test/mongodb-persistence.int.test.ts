import { MongoDBContainer, type StartedMongoDBContainer } from '@testcontainers/mongodb';
import { MongoClient, type ClientSession } from 'mongodb';
import { afterAll, beforeAll, describe, expect, test } from 'vitest';
import { MongoDBPersistence } from '@zusammen/mongodb';
import { persistenceProviderContract, sampleRecord } from '@zusammen/testing';

let container: StartedMongoDBContainer;
let client: MongoClient;
let database = 0;

beforeAll(async () => {
  container = await new MongoDBContainer('mongo:8').start();
  client = new MongoClient(container.getConnectionString(), { directConnection: true });
  await client.connect();
}, 120_000);

afterAll(async () => {
  await client.close();
  await container.stop();
});

/** A provider on a fresh database, so tests don't see each other's data. */
async function createPersistence(options: { retentionMs?: number } = {}) {
  const databaseName = `zusammen_${String(++database)}`;
  const persistence = new MongoDBPersistence({ client, databaseName, ...options });
  await persistence.connect();
  return { persistence, databaseName };
}

persistenceProviderContract<ClientSession>('mongodb', {
  setup: async () => {
    const { persistence, databaseName } = await createPersistence();
    const business = client.db(databaseName).collection<{ _id: string; value: string }>('business');
    return {
      persistence,
      write: async (session, key, value) => {
        await business.insertOne({ _id: key, value }, { session });
      },
      read: async (key) => (await business.findOne({ _id: key }))?.value,
    };
  },
});

describe('mongodb persistence', () => {
  test('creates a TTL index for dispatched records and tombstones', async () => {
    const { persistence } = await createPersistence({ retentionMs: 60_000 });

    const indexes = await persistence.collection.indexes();
    expect(indexes).toContainEqual(
      expect.objectContaining({ name: 'zusammen_dispatched_ttl', key: { DispatchedAt: 1 }, expireAfterSeconds: 60 }),
    );
  });

  test('updates the TTL index when the retention changes', async () => {
    const { databaseName } = await createPersistence({ retentionMs: 60_000 });
    const updated = new MongoDBPersistence({ client, databaseName, retentionMs: 120_000 });
    await updated.connect();

    const indexes = await updated.collection.indexes();
    expect(indexes.find((index) => index.name === 'zusammen_dispatched_ttl')?.expireAfterSeconds).toBe(120);
  });

  test('stores headers with dots and dollars as key/value pairs', async () => {
    const { persistence } = await createPersistence();
    const record = sampleRecord();
    const session = await persistence.begin();
    await persistence.storeOutbox(record, session);
    await persistence.commit(session);

    const raw = await persistence.collection.findOne({ _id: record.id });
    expect(raw?.TransportOperations[0]?.Headers).toContainEqual({ Key: 'with.dots', Value: 'and $dollars' });
    expect(raw?.Dispatched).toBe(false);
    expect(raw).not.toHaveProperty('DispatchedAt');
  });

  test('a tombstone waits for an in-flight transaction that already inserted the record', async () => {
    const { persistence } = await createPersistence();
    const record = sampleRecord();
    const session = await persistence.begin();
    await persistence.storeOutbox(record, session);

    let settled = false;
    const tombstone = persistence.storeTombstone(record.id).finally(() => {
      settled = true;
    });
    await new Promise((resolve) => setTimeout(resolve, 300));
    expect(settled).toBe(false);

    await persistence.commit(session);
    expect(await tombstone).toBe('exists');
    expect(await persistence.get(record.id)).toEqual(record);
  });

  test('rollback after an ended session is a no-op', async () => {
    const { persistence } = await createPersistence();
    const session = await persistence.begin();
    await persistence.commit(session);

    await expect(persistence.rollback(session)).resolves.toBeUndefined();
  });

  test('does not close a shared client on disconnect', async () => {
    const { persistence } = await createPersistence();
    await persistence.disconnect();

    await expect(client.db('admin').command({ ping: 1 })).resolves.toMatchObject({ ok: 1 });
  });
});
