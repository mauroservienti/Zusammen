import type { Channel, Options } from 'amqplib';
import type { MessageConvention, TransportOperation } from '@zusammen/core';

/** Where an operation is published. */
export interface Route {
  exchange: string;
  routingKey: string;
  /**
   * The exchange may legitimately not exist yet, e.g. an event exchange created by subscribers: if it's missing, the
   * message has no subscribers and is skipped. With resource creation enabled, it's declared with this type instead.
   */
  optionalExchange?: { type: 'fanout' | 'topic' };
}

/** Decides where outgoing messages go and which exchanges it needs. */
export interface RoutingTopology {
  readonly name: string;
  /** Exchanges that must exist before publishing, verified at startup. */
  requiredExchanges(): string[];
  /** Declares the required exchanges; only called when resource creation is enabled. */
  createResources(channel: Channel): Promise<void>;
  route(operation: TransportOperation): Route;
  /** Rejects conventions the topology can't route, at startup. */
  validateConvention?(convention: MessageConvention): void;
  /** AMQP properties for an operation; defaults to the operation's ID, type, content type, headers and properties. */
  publishOptions?(operation: TransportOperation): Options.Publish;
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
    requiredExchanges: () => [eventsExchange],
    async createResources(channel) {
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
