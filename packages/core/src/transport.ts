import type { ControlMessage, ControlMessageHandler } from './control.js';
import type { MessageConvention } from './convention.js';
import type { TransportOperation } from './outbox.js';

/** Stops control message processing; resolves once in-flight messages are done. */
export type StopControlProcessing = () => Promise<void>;

export interface TransportProvider {
  connect(): Promise<void>;
  disconnect(): Promise<void>;

  /** Resolves only once the broker has accepted every operation. Unroutable sends reject with {@link UnroutableMessageError}. */
  dispatch(operations: readonly TransportOperation[]): Promise<void>;
  /** Resolves only once the broker has accepted the control message. */
  sendControl(message: ControlMessage): Promise<void>;
  /** Starts delivering control messages to the handler and applies each {@link ControlResult} natively. */
  consumeControl(handler: ControlMessageHandler): Promise<StopControlProcessing>;

  /** Called at startup; throws {@link IncompatibleConventionError} when the routing topology can't work with the convention. */
  validateConvention?(convention: MessageConvention): void;
}
