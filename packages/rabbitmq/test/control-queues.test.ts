import { describe, expect, test } from 'vitest';
import { controlQueueNames, delayLevelFor } from '@zusammen/rabbitmq';

describe('control queues', () => {
  test.each([
    [0, 0],
    [-5, 0],
    [1, 1],
    [1_000, 1],
    [1_001, 2],
    [3_000, 4],
    [10_000, 16],
    [60_000, 64],
    [500_000, 64],
  ])('a %i ms delay uses the %i s level', (delayMs, level) => {
    expect(delayLevelFor(delayMs)).toBe(level);
  });

  test('queue names derive from the control queue', () => {
    const names = controlQueueNames('orders.control');
    expect(names.error).toBe('orders.control.error');
    expect(names.delay(4)).toBe('orders.control.delay.4s');
  });
});
