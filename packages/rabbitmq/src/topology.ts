import type { Channel } from 'amqplib';
import type { MessageConvention, TransportOperation } from '@zusammen/core';

/** Where an operation is published. */
export interface Route {
  exchange: string;
  routingKey: string;
}

/** Decides where outgoing messages go and which exchanges exist. */
export interface RoutingTopology {
  readonly name: string;
  /** Declares the exchanges the topology publishes to; called after every (re)connect. */
  declare(channel: Channel): Promise<void>;
  route(operation: TransportOperation): Route;
  /** Rejects conventions the topology can't route, at startup. */
  validateConvention?(convention: MessageConvention): void;
}

export const ZUSAMMEN_EVENTS_EXCHANGE = 'zusammen.events';

export interface ZusammenTopologyOptions {
  /** Topic exchange for published events. Defaults to {@link ZUSAMMEN_EVENTS_EXCHANGE}. */
  eventsExchange?: string | undefined;
}

/**
 * The default topology. Sends go through the default exchange to the queue named after the destination; publishes go
 * to a durable topic exchange with the topic (or the message type) as routing key.
 */
export function zusammenTopology(options: ZusammenTopologyOptions = {}): RoutingTopology {
  const eventsExchange = options.eventsExchange ?? ZUSAMMEN_EVENTS_EXCHANGE;
  return {
    name: 'zusammen',
    async declare(channel) {
      await channel.assertExchange(eventsExchange, 'topic', { durable: true });
    },
    route(operation) {
      if (operation.intent === 'send') {
        return { exchange: '', routingKey: operation.destination ?? '' };
      }
      return { exchange: eventsExchange, routingKey: operation.topic ?? operation.messageType };
    },
  };
}
