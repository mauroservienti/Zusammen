import type { Channel } from 'amqplib';
import type { ControlMessage } from '@zusammen/core';

/** Delay levels, in seconds. Requested delays are rounded up to the next level. */
export const DELAY_LEVELS_SECONDS = [1, 2, 4, 8, 16, 32, 64] as const;
const MAX_DELAY_LEVEL_SECONDS = 64;

export type QueueType = 'quorum' | 'classic';

export interface ControlQueueNames {
  control: string;
  error: string;
  delay(levelSeconds: number): string;
}

export function allControlQueues(names: ControlQueueNames): string[] {
  return [names.control, names.error, ...DELAY_LEVELS_SECONDS.map((level) => names.delay(level))];
}

export function controlQueueNames(control: string): ControlQueueNames {
  return {
    control,
    error: `${control}.error`,
    delay: (levelSeconds) => `${control}.delay.${String(levelSeconds)}s`,
  };
}

/**
 * The delay level for a requested delay; never shorter than requested, so a commit always gets at least its full
 * window. Delays beyond the largest level are capped (and repeated by the handler if needed). Zero means no delay.
 */
export function delayLevelFor(delayMs: number): number {
  if (delayMs <= 0) {
    return 0;
  }
  const seconds = delayMs / 1_000;
  return DELAY_LEVELS_SECONDS.find((level) => level >= seconds) ?? MAX_DELAY_LEVEL_SECONDS;
}

/**
 * Control, error and delay queues. Each delay level is its own classic queue with a queue-level TTL that dead-letters
 * back to the control queue: per-message TTLs would expire only at the head of a shared queue.
 */
export async function declareControlQueues(channel: Channel, names: ControlQueueNames, queueType: QueueType) {
  const queueTypeArgument = { 'x-queue-type': queueType };
  await channel.assertQueue(names.control, { durable: true, arguments: queueTypeArgument });
  await channel.assertQueue(names.error, { durable: true, arguments: queueTypeArgument });
  for (const level of DELAY_LEVELS_SECONDS) {
    await channel.assertQueue(names.delay(level), {
      durable: true,
      arguments: {
        'x-queue-type': 'classic',
        'x-message-ttl': level * 1_000,
        'x-dead-letter-exchange': '',
        'x-dead-letter-routing-key': names.control,
      },
    });
  }
}

export const CONTROL_MESSAGE_TYPE = 'zusammen.control';

export function encodeControlMessage(message: ControlMessage): Buffer {
  return Buffer.from(JSON.stringify(message));
}

export function decodeControlMessage(content: Buffer): ControlMessage {
  const value: unknown = JSON.parse(content.toString('utf8'));
  if (!isControlMessage(value)) {
    throw new Error('Malformed control message');
  }
  return value;
}

function isControlMessage(value: unknown): value is ControlMessage {
  if (typeof value !== 'object' || value === null) {
    return false;
  }
  const candidate = value as Record<string, unknown>;
  return (
    typeof candidate.sessionId === 'string' &&
    typeof candidate.remainingCommitDurationMs === 'number' &&
    typeof candidate.commitDelayIncrementMs === 'number' &&
    typeof candidate.attempt === 'number' &&
    typeof candidate.failures === 'number'
  );
}
