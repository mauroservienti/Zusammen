/** Sent before the outbox transaction commits; guarantees dispatch even if the committing process dies. */
export interface ControlMessage {
  sessionId: string;
  /** Time left for the session's transaction to commit. Decremented by every retry delay; at or below 0 the window has expired. */
  remainingCommitDurationMs: number;
  /** Delay before the next retry while waiting for the commit; grows with every attempt. */
  commitDelayIncrementMs: number;
  /** 1-based processing attempt, counting every delivery. */
  attempt: number;
  /** Attempts that failed with an error (e.g. dispatch failures); the handler gives up once this reaches its limit. */
  failures: number;
}

/** Outcome of processing a control message; each transport maps it to native actions (ack, delayed republish, dead-letter). */
export type ControlResult =
  { kind: 'ack' } | { kind: 'retry'; delayMs: number; next: ControlMessage } | { kind: 'error'; error: Error };

export type ControlMessageHandler = (message: ControlMessage) => Promise<ControlResult>;
