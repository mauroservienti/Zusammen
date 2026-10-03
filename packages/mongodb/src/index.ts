export type { OperationDocument, OutboxDocument } from './documents.js';
export {
  DEFAULT_OUTBOX_COLLECTION,
  DEFAULT_RETENTION_MS,
  MongoDBPersistence,
  type MongoDBPersistenceOptions,
} from './persistence.js';
