import type { Serializer } from '@zusammen/core';

export type PropertyNaming = 'pascal' | 'preserve';

const toPascalCase = (name: string) => name.charAt(0).toUpperCase() + name.slice(1);

/**
 * JSON for NServiceBus' System.Text.Json serializer, which matches property names case-sensitively by default:
 * PascalCase property names bind to .NET properties.
 */
export function nserviceBusJsonSerializer(propertyNaming: PropertyNaming = 'pascal'): Serializer {
  const encoder = new TextEncoder();
  const replacer =
    propertyNaming === 'pascal'
      ? (_key: string, value: unknown): unknown => {
          if (typeof value !== 'object' || value === null || Array.isArray(value)) {
            return value;
          }
          return Object.fromEntries(Object.entries(value).map(([name, inner]) => [toPascalCase(name), inner]));
        }
      : undefined;
  return {
    contentType: 'application/json',
    serialize: (message) => encoder.encode(JSON.stringify(message, replacer)),
  };
}
