/**
 * The MongoDB image integration tests run against. CI picks MongoDB 7 on Linux 6.19+, where MongoDB 8 refuses to
 * start (https://jira.mongodb.org/browse/SERVER-121912).
 */
export const MONGODB_IMAGE = process.env.ZUSAMMEN_MONGODB_IMAGE ?? 'mongo:8';
