import {
  SessionCommitConflictError,
  type ControlMessage,
  type ControlMessageHandler,
  type ControlResult,
  type MessageConvention,
  type OutboxRecord,
  type PersistenceProvider,
  type StopControlProcessing,
  type TransportOperation,
  type TransportProvider,
} from '@zusammen/core';

export interface InMemoryTransaction {
  outbox?: OutboxRecord;
  writes: Map<string, unknown>;
}

/** Transactional in-memory persistence with a business key-value store, mimicking insert conflicts on the outbox. */
export class InMemoryPersistence implements PersistenceProvider<InMemoryTransaction> {
  readonly records = new Map<string, OutboxRecord>();
  readonly data = new Map<string, unknown>();
  /** Number of upcoming `get` calls that return null even if the record exists (simulates storage lag). */
  staleReads = 0;
  /** Runs inside `commit` before the conflict check, to interleave control message processing. */
  beforeCommit: (() => Promise<void>) | undefined;
  connected = false;

  connect(): Promise<void> {
    this.connected = true;
    return Promise.resolve();
  }

  disconnect(): Promise<void> {
    this.connected = false;
    return Promise.resolve();
  }

  begin(): Promise<InMemoryTransaction> {
    return Promise.resolve({ writes: new Map() });
  }

  storeOutbox(record: OutboxRecord, tx: InMemoryTransaction): Promise<void> {
    if (this.records.has(record.id)) {
      return Promise.reject(new SessionCommitConflictError(record.id));
    }
    tx.outbox = structuredClone(record);
    return Promise.resolve();
  }

  async commit(tx: InMemoryTransaction): Promise<void> {
    await this.beforeCommit?.();
    if (tx.outbox !== undefined) {
      if (this.records.has(tx.outbox.id)) {
        throw new SessionCommitConflictError(tx.outbox.id);
      }
      this.records.set(tx.outbox.id, tx.outbox);
    }
    for (const [key, value] of tx.writes) {
      this.data.set(key, value);
    }
  }

  rollback(tx: InMemoryTransaction): Promise<void> {
    delete tx.outbox;
    tx.writes.clear();
    return Promise.resolve();
  }

  get(sessionId: string): Promise<OutboxRecord | null> {
    if (this.staleReads > 0) {
      this.staleReads--;
      return Promise.resolve(null);
    }
    const record = this.records.get(sessionId);
    return Promise.resolve(record === undefined ? null : structuredClone(record));
  }

  markDispatched(sessionId: string): Promise<void> {
    const record = this.records.get(sessionId);
    if (record !== undefined) {
      record.dispatched = true;
      record.dispatchedAt = new Date();
    }
    return Promise.resolve();
  }

  storeTombstone(sessionId: string): Promise<'stored' | 'exists'> {
    if (this.records.has(sessionId)) {
      return Promise.resolve('exists');
    }
    this.records.set(sessionId, { id: sessionId, dispatched: true, dispatchedAt: new Date(), transportOperations: [] });
    return Promise.resolve('stored');
  }
}

export interface ProcessedControlMessage {
  message: ControlMessage;
  result: ControlResult;
}

/** In-memory transport; control messages are processed explicitly by the test, one at a time. */
export class InMemoryTransport implements TransportProvider {
  readonly dispatched: TransportOperation[] = [];
  readonly controlQueue: ControlMessage[] = [];
  readonly controlDelays: number[] = [];
  readonly processed: ProcessedControlMessage[] = [];
  readonly deadLettered: ControlMessage[] = [];
  /** Number of upcoming `dispatch` calls that fail. */
  failingDispatches = 0;
  failSendControl = false;
  handler: ControlMessageHandler | undefined;
  validateConvention?: (convention: MessageConvention) => void;
  connected = false;

  connect(): Promise<void> {
    this.connected = true;
    return Promise.resolve();
  }

  disconnect(): Promise<void> {
    this.connected = false;
    return Promise.resolve();
  }

  dispatch(operations: readonly TransportOperation[]): Promise<void> {
    if (this.failingDispatches > 0) {
      this.failingDispatches--;
      return Promise.reject(new Error('broker unavailable'));
    }
    this.dispatched.push(...operations);
    return Promise.resolve();
  }

  sendControl(message: ControlMessage, delayMs: number): Promise<void> {
    if (this.failSendControl) {
      return Promise.reject(new Error('broker unavailable'));
    }
    this.controlQueue.push(message);
    this.controlDelays.push(delayMs);
    return Promise.resolve();
  }

  consumeControl(handler: ControlMessageHandler): Promise<StopControlProcessing> {
    this.handler = handler;
    return Promise.resolve(() => {
      this.handler = undefined;
      return Promise.resolve();
    });
  }

  /** Processes the next control message and applies its result like a broker would. */
  async processNext(): Promise<ProcessedControlMessage> {
    const message = this.controlQueue.shift();
    if (message === undefined || this.handler === undefined) {
      throw new Error('No control message or no consumer');
    }
    const result = await this.handler(message);
    if (result.kind === 'retry') {
      this.controlQueue.push(result.next);
    } else if (result.kind === 'error') {
      this.deadLettered.push(message);
    }
    const processed = { message, result };
    this.processed.push(processed);
    return processed;
  }

  /** Processes control messages until the queue is empty. */
  async drain(maxSteps = 100): Promise<void> {
    for (let step = 0; this.controlQueue.length > 0; step++) {
      if (step >= maxSteps) {
        throw new Error('Control queue did not drain');
      }
      await this.processNext();
    }
  }
}
