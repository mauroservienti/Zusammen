import { Binary } from 'mongodb';
import type { OutboxRecord, TransportOperation } from '@zusammen/core';

// Field names mirror NServiceBus.Storage.MongoDB for familiarity; the format is Zusammen-specific.
export interface OutboxDocument {
  _id: string;
  Dispatched: boolean;
  DispatchedAt?: Date;
  TransportOperations: OperationDocument[];
}

export interface OperationDocument {
  MessageId: string;
  Intent: TransportOperation['intent'];
  Destination?: string;
  Topic?: string;
  MessageType: string;
  // Key/value pairs rather than an object: header names contain dots, which MongoDB field names handle poorly
  Headers: { Key: string; Value: string }[];
  Body: Binary;
  Properties: { ContentType: string; CorrelationId?: string; ReplyTo?: string };
}

export function toDocument(record: OutboxRecord): OutboxDocument {
  const document: OutboxDocument = {
    _id: record.id,
    Dispatched: record.dispatched,
    TransportOperations: record.transportOperations.map(toOperationDocument),
  };
  if (record.dispatchedAt !== undefined) {
    document.DispatchedAt = record.dispatchedAt;
  }
  return document;
}

export function fromDocument(document: OutboxDocument): OutboxRecord {
  const record: OutboxRecord = {
    id: document._id,
    dispatched: document.Dispatched,
    transportOperations: document.TransportOperations.map(fromOperationDocument),
  };
  if (document.DispatchedAt !== undefined) {
    record.dispatchedAt = document.DispatchedAt;
  }
  return record;
}

function toOperationDocument(operation: TransportOperation): OperationDocument {
  const document: OperationDocument = {
    MessageId: operation.messageId,
    Intent: operation.intent,
    MessageType: operation.messageType,
    Headers: Object.entries(operation.headers).map(([Key, Value]) => ({ Key, Value })),
    Body: new Binary(operation.body),
    Properties: { ContentType: operation.properties.contentType },
  };
  if (operation.destination !== undefined) document.Destination = operation.destination;
  if (operation.topic !== undefined) document.Topic = operation.topic;
  if (operation.properties.correlationId !== undefined) {
    document.Properties.CorrelationId = operation.properties.correlationId;
  }
  if (operation.properties.replyTo !== undefined) document.Properties.ReplyTo = operation.properties.replyTo;
  return document;
}

function fromOperationDocument(document: OperationDocument): TransportOperation {
  const operation: TransportOperation = {
    messageId: document.MessageId,
    intent: document.Intent,
    messageType: document.MessageType,
    headers: Object.fromEntries(document.Headers.map(({ Key, Value }) => [Key, Value])),
    // Copy: the driver's buffer may be a view over a larger pooled allocation
    body: new Uint8Array(document.Body.buffer.subarray(0, document.Body.position)),
    properties: { contentType: document.Properties.ContentType },
  };
  if (document.Destination !== undefined) operation.destination = document.Destination;
  if (document.Topic !== undefined) operation.topic = document.Topic;
  if (document.Properties.CorrelationId !== undefined) {
    operation.properties.correlationId = document.Properties.CorrelationId;
  }
  if (document.Properties.ReplyTo !== undefined) operation.properties.replyTo = document.Properties.ReplyTo;
  return operation;
}
