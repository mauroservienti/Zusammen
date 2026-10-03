import { InMemoryPersistence, persistenceProviderContract, type InMemoryTransaction } from '@zusammen/testing';

persistenceProviderContract<InMemoryTransaction>('in-memory', {
  setup: () => {
    const persistence = new InMemoryPersistence();
    return Promise.resolve({
      persistence,
      write: (context, key, value) => {
        context.writes.set(key, value);
        return Promise.resolve();
      },
      read: (key) => Promise.resolve(persistence.data.get(key) as string | undefined),
    });
  },
});
