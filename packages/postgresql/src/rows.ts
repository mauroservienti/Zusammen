import type { OutboxRecord, TransportOperation } from '@zusammen/core';

export interface OutboxRow {
  id: string;
  dispatched: boolean;
  dispatched_at: Date | null;
  transport_operations: OperationJson[];
}

/** A transport operation as stored in the `transport_operations` JSON column; the body is base64. */
export interface OperationJson {
  messageId: string;
  intent: TransportOperation['intent'];
  destination?: string;
  topic?: string;
  messageType: string;
  headers: Record<string, string>;
  body: string;
  properties: TransportOperation['properties'];
}

export function toOperationsJson(operations: readonly TransportOperation[]): string {
  return JSON.stringify(
    operations.map((operation): OperationJson => ({
      ...operation,
      body: Buffer.from(operation.body.buffer, operation.body.byteOffset, operation.body.byteLength).toString('base64'),
    })),
  );
}

export function fromRow(row: OutboxRow): OutboxRecord {
  const record: OutboxRecord = {
    id: row.id,
    dispatched: row.dispatched,
    transportOperations: row.transport_operations.map(fromOperationJson),
  };
  if (row.dispatched_at !== null) {
    record.dispatchedAt = row.dispatched_at;
  }
  return record;
}

function fromOperationJson(json: OperationJson): TransportOperation {
  const operation: TransportOperation = {
    messageId: json.messageId,
    intent: json.intent,
    messageType: json.messageType,
    headers: json.headers,
    // A plain Uint8Array copy rather than a Buffer
    body: new Uint8Array(Buffer.from(json.body, 'base64')),
    properties: { contentType: json.properties.contentType },
  };
  if (json.destination !== undefined) operation.destination = json.destination;
  if (json.topic !== undefined) operation.topic = json.topic;
  if (json.properties.correlationId !== undefined) operation.properties.correlationId = json.properties.correlationId;
  if (json.properties.replyTo !== undefined) operation.properties.replyTo = json.properties.replyTo;
  return operation;
}
