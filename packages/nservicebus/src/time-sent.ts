const pad = (value: number, length = 2) => String(value).padStart(length, '0');

/**
 * NServiceBus wire format for dates: `yyyy-MM-dd HH:mm:ss:ffffff Z` in UTC (note the `:` before the fraction). JS dates
 * have millisecond precision, the remaining microsecond digits are zero.
 */
export function toWireFormat(date: Date): string {
  return (
    `${pad(date.getUTCFullYear(), 4)}-${pad(date.getUTCMonth() + 1)}-${pad(date.getUTCDate())} ` +
    `${pad(date.getUTCHours())}:${pad(date.getUTCMinutes())}:${pad(date.getUTCSeconds())}:` +
    `${pad(date.getUTCMilliseconds(), 3)}000 Z`
  );
}
