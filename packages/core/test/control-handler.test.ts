import { describe, expect, test } from 'vitest';
import {
  createControlMessageHandler,
  silentLogger,
  type ControlMessage,
  type TransportOperation,
} from '@zusammen/core';
import { InMemoryPersistence, InMemoryTransport } from '@zusammen/testing';

const operation: TransportOperation = {
  messageId: 'm1',
  intent: 'publish',
  messageType: 'OrderPlaced',
  headers: {},
  body: new Uint8Array(),
  properties: { contentType: 'application/json' },
};

const control = (overrides: Partial<ControlMessage> = {}): ControlMessage => ({
  sessionId: 's1',
  remainingCommitDurationMs: 15_000,
  commitDelayIncrementMs: 1_000,
  attempt: 1,
  failures: 0,
  ...overrides,
});

function setup() {
  const persistence = new InMemoryPersistence();
  const transport = new InMemoryTransport();
  const handler = createControlMessageHandler({
    persistence,
    transport,
    logger: silentLogger,
    timing: { maxCommitDelayIncrementMs: 4_000, maxFailures: 3, initialFailureDelayMs: 100, maxFailureDelayMs: 150 },
  });
  return { persistence, transport, handler };
}

describe('control message handler', () => {
  test('dispatched record: ack without dispatching', async () => {
    const { persistence, transport, handler } = setup();
    persistence.records.set('s1', { id: 's1', dispatched: true, transportOperations: [operation] });

    expect(await handler(control())).toEqual({ kind: 'ack' });
    expect(transport.dispatched).toHaveLength(0);
  });

  test('pending record: dispatch, mark dispatched, ack', async () => {
    const { persistence, transport, handler } = setup();
    persistence.records.set('s1', { id: 's1', dispatched: false, transportOperations: [operation] });

    expect(await handler(control())).toEqual({ kind: 'ack' });
    expect(transport.dispatched).toEqual([operation]);
    expect(persistence.records.get('s1')?.dispatched).toBe(true);
  });

  test('pending record with an expired window: still dispatched', async () => {
    const { persistence, transport, handler } = setup();
    persistence.records.set('s1', { id: 's1', dispatched: false, transportOperations: [operation] });

    expect(await handler(control({ remainingCommitDurationMs: 0 }))).toEqual({ kind: 'ack' });
    expect(transport.dispatched).toHaveLength(1);
  });

  test('missing record within the window: retry with growing, capped delays that consume the window', async () => {
    const { handler } = setup();

    const delays: number[] = [];
    let message = control({ remainingCommitDurationMs: 10_000 });
    for (;;) {
      const result = await handler(message);
      if (result.kind !== 'retry') break;
      delays.push(result.delayMs);
      message = result.next;
    }

    expect(delays).toEqual([1_000, 2_000, 4_000, 3_000]);
    expect(message).toMatchObject({ remainingCommitDurationMs: 0, attempt: 5, failures: 0 });
  });

  test('missing record after the window: store tombstone, ack', async () => {
    const { persistence, handler } = setup();

    expect(await handler(control({ remainingCommitDurationMs: 0 }))).toEqual({ kind: 'ack' });
    expect(persistence.records.get('s1')).toMatchObject({ dispatched: true, transportOperations: [] });
  });

  test('commit lands while storing the tombstone: dispatch the committed record', async () => {
    const { persistence, transport, handler } = setup();
    persistence.staleReads = 1;
    persistence.records.set('s1', { id: 's1', dispatched: false, transportOperations: [operation] });

    expect(await handler(control({ remainingCommitDurationMs: 0 }))).toEqual({ kind: 'ack' });
    expect(transport.dispatched).toEqual([operation]);
  });

  test('stale read within the window is treated as a pending commit, never as missing', async () => {
    const { persistence, transport, handler } = setup();
    persistence.staleReads = 1;
    persistence.records.set('s1', { id: 's1', dispatched: false, transportOperations: [operation] });

    const first = await handler(control());
    expect(first.kind).toBe('retry');
    if (first.kind === 'retry') {
      expect(await handler(first.next)).toEqual({ kind: 'ack' });
    }
    expect(transport.dispatched).toHaveLength(1);
  });

  test('failures retry with capped backoff, then give up', async () => {
    const { persistence, transport, handler } = setup();
    persistence.records.set('s1', { id: 's1', dispatched: false, transportOperations: [operation] });
    transport.failingDispatches = Number.POSITIVE_INFINITY;

    const first = await handler(control());
    expect(first).toMatchObject({ kind: 'retry', delayMs: 100, next: { failures: 1, attempt: 2 } });
    if (first.kind !== 'retry') return;

    const second = await handler(first.next);
    expect(second).toMatchObject({ kind: 'retry', delayMs: 150, next: { failures: 2 } });
    if (second.kind !== 'retry') return;

    const third = await handler(second.next);
    expect(third).toMatchObject({ kind: 'error', error: { message: 'broker unavailable' } });
  });

  test('failures do not consume the commit window', async () => {
    const { persistence, handler } = setup();
    persistence.get = () => Promise.reject(new Error('db down'));

    const result = await handler(control({ remainingCommitDurationMs: 5_000 }));
    expect(result).toMatchObject({ kind: 'retry', next: { remainingCommitDurationMs: 5_000, failures: 1 } });
  });
});
