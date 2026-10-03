import { afterAll, afterEach, beforeAll, describe, expect, test } from 'vitest';
import { nserviceBusConvention } from '@zusammen/nservicebus';
import { nserviceBusConventionalTopology, nserviceBusDirectTopology } from '@zusammen/rabbitmq/nservicebus';
import { Environment, sleep, unique, waitFor } from './environment.js';
import { buildEndpoint, dotnetAvailable, startEndpoint } from './nservicebus-endpoint.js';

// In CI, missing .NET must fail the run instead of silently skipping compatibility tests
const required = process.env.ZUSAMMEN_REQUIRE_DOTNET === 'true';

class PlaceOrder {
  constructor(
    readonly orderId: string,
    readonly lines: { productId: string; quantity: number }[],
  ) {}
}

class OrderPlaced {
  constructor(readonly orderId: string) {}
}

const topologies = {
  conventional: { create: nserviceBusConventionalTopology, orderPlacedTopic: 'Sales.Messages:OrderPlaced' },
  direct: { create: nserviceBusDirectTopology, orderPlacedTopic: 'Sales-Messages-OrderPlaced' },
} as const;

const mapped = new Map<abstract new (...args: never[]) => unknown, string>([
  [PlaceOrder, 'Sales.Messages.PlaceOrder'],
  [OrderPlaced, 'Sales.Messages.OrderPlaced'],
]);

describe.skipIf(!dotnetAvailable && !required)('NServiceBus compatibility', () => {
  const environment = new Environment();
  const endpoints: { stop(): Promise<void> }[] = [];

  beforeAll(async () => {
    buildEndpoint();
    await environment.start();
  }, 300_000);
  afterEach(async () => {
    await Promise.all(endpoints.splice(0).map((endpoint) => endpoint.stop()));
    await environment.stopFactories();
  });
  afterAll(() => environment.stop());

  async function setup(
    topologyName: keyof typeof topologies,
    options: { zusammen?: boolean; mapTypes?: boolean; outbox?: boolean } = {},
  ) {
    const name = unique('Sales').replaceAll('.', '-');
    const endpoint = await startEndpoint({
      amqpUrl: environment.amqpUrl,
      managementUrl: environment.managementUrl,
      name,
      topology: topologyName,
      mongoConnectionString: environment.mongoConnectionString,
      zusammen: options.zusammen ?? false,
      outbox: options.outbox ?? false,
    });
    endpoints.push(endpoint);
    const topology = topologies[topologyName];
    const instance = await environment.instance({
      database: environment.client.db(unique('zusammen').replaceAll('.', '_')),
      controlQueue: unique('control'),
      topology: topology.create(),
      convention: nserviceBusConvention({
        endpointName: 'Zusammen.Tests',
        ...(options.mapTypes !== false && { messageTypes: mapped }),
        topics: { 'Sales.Messages.OrderPlaced': topology.orderPlacedTopic, OrderPlaced: topology.orderPlacedTopic },
      }),
    });
    return { endpoint, name, factory: instance.factory };
  }

  describe.each(['conventional', 'direct'] as const)('%s topology', (topologyName) => {
    test('a mapped command is handled, with properties bound and NServiceBus headers', async () => {
      const { endpoint, name, factory } = await setup(topologyName);
      const orderId = unique('order');

      const session = await factory.open();
      await session.send(name, new PlaceOrder(orderId, [{ productId: 'p1', quantity: 3 }]));
      await session.commit();

      await waitFor(() => endpoint.handled().length === 1, 30_000);
      const [handled] = endpoint.handled();
      expect(handled?.type).toBe('Sales.Messages.PlaceOrder');
      expect(handled?.body).toEqual({ OrderId: orderId, Lines: [{ ProductId: 'p1', Quantity: 3 }] });
      expect(handled?.headers).toMatchObject({
        'NServiceBus.OriginatingEndpoint': 'Zusammen.Tests',
        'NServiceBus.ConversationId': session.sessionId,
        'NServiceBus.MessageIntent': 'Send',
      });
    });

    test('a mapped event is delivered to the subscriber', async () => {
      const { endpoint, factory } = await setup(topologyName);
      const orderId = unique('order');

      const session = await factory.open();
      await session.publish(new OrderPlaced(orderId));
      await session.commit();

      await waitFor(() => endpoint.handled().some((report) => report.body?.OrderId === orderId), 30_000);
    });

    test('unmapped messages are resolved by the Zusammen.NServiceBus behavior', async () => {
      const { endpoint, name, factory } = await setup(topologyName, { zusammen: true, mapTypes: false });
      const orderId = unique('order');

      const session = await factory.open();
      await session.send(name, new PlaceOrder(orderId, []));
      await session.publish(new OrderPlaced(orderId));
      await session.commit();

      await waitFor(() => endpoint.handled().filter((report) => report.body?.OrderId === orderId).length === 2, 30_000);
      expect(
        endpoint
          .handled()
          .map((report) => report.type)
          .sort(),
      ).toEqual(['Sales.Messages.OrderPlaced', 'Sales.Messages.PlaceOrder']);
    });

    test('unmapped messages fail on receivers without the behavior', async () => {
      const { endpoint, name, factory } = await setup(topologyName, { mapTypes: false });

      const session = await factory.open();
      await session.send(name, new PlaceOrder(unique('order'), []));
      await session.commit();

      await waitFor(() => endpoint.failed().length === 1, 30_000);
      expect(endpoint.handled()).toHaveLength(0);
    });
  });

  test('the NServiceBus outbox deduplicates a message dispatched again later', async () => {
    const { endpoint, name, factory } = await setup('conventional', { outbox: true });
    const orderId = unique('order');
    // Same message ID, as when a control message re-dispatches an already delivered message
    const sendOnce = async () => {
      const session = await factory.open();
      await session.send(name, new PlaceOrder(orderId, []), { messageId: `dedup-${orderId}` });
      await session.commit();
    };

    await sendOnce();
    await waitFor(() => endpoint.handled().length === 1, 30_000);
    // Handlers report before the endpoint's outbox transaction commits; re-dispatches come seconds later anyway
    await sleep(2_000);
    await sendOnce();

    await sleep(5_000);
    expect(endpoint.handled()).toHaveLength(1);
    expect(endpoint.failed()).toHaveLength(0);
  }, 60_000);
});
