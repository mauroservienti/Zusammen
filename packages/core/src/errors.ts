/** Base class for all Zusammen errors. */
export class ZusammenError extends Error {
  constructor(message: string, options?: ErrorOptions) {
    super(message, options);
    this.name = new.target.name;
  }
}

/** The session's outbox record already exists, typically a tombstone stored because the commit window expired. */
export class SessionCommitConflictError extends ZusammenError {
  constructor(
    readonly sessionId: string,
    options?: ErrorOptions,
  ) {
    super(
      `Failed to commit transactional session '${sessionId}': its outbox record already exists. This happens when the commit takes longer than the maximum commit duration.`,
      options,
    );
  }
}

/** The session was already committed or rolled back. */
export class SessionClosedError extends ZusammenError {
  constructor(
    readonly sessionId: string,
    readonly state: 'committing' | 'committed' | 'rolledBack',
  ) {
    super(`Transactional session '${sessionId}' is already ${state === 'rolledBack' ? 'rolled back' : state}.`);
  }
}

export class FactoryNotStartedError extends ZusammenError {
  constructor() {
    super('The session factory must be started before opening sessions. Call start() first.');
  }
}

/** The convention can't determine a message type for the outgoing message. */
export class UnknownMessageTypeError extends ZusammenError {
  constructor(
    readonly convention: string,
    detail: string,
  ) {
    super(`Convention '${convention}' cannot determine the message type: ${detail}`);
  }
}

/** A publish has no topic and the convention or topology requires one. */
export class UnknownPublishTopicError extends ZusammenError {
  constructor(readonly messageType: string) {
    super(
      `No publish topic for message type '${messageType}'. Pass the 'topic' option or register a topic for the type.`,
    );
  }
}

/** The broker could not route a send to any destination. */
export class UnroutableMessageError extends ZusammenError {
  constructor(
    readonly messageId: string,
    readonly destination: string,
    options?: ErrorOptions,
  ) {
    super(`Message '${messageId}' could not be routed to '${destination}'.`, options);
  }
}

/** Resources the providers need (collections, queues, exchanges, …) don't exist and resource creation is off. */
export class MissingResourcesError extends ZusammenError {
  constructor(readonly resources: readonly string[]) {
    super(
      `Missing resources: ${resources.join(', ')}. Create them as part of your deployment, or enable resource creation (createResources: true).`,
    );
  }
}

/** The transport's routing topology cannot work with the configured convention. */
export class IncompatibleConventionError extends ZusammenError {
  constructor(
    readonly convention: string,
    readonly requirement: string,
  ) {
    super(`The transport cannot be used with the '${convention}' convention: ${requirement}`);
  }
}
