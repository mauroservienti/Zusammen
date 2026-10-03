# Sample: Node.js → Node.js

An orders API stores an order and publishes `OrderPlaced` in one transactional session. A shipping worker receives the event and creates a shipment, deduplicating by message ID because Zusammen delivers at least once.

- `src/api.ts`: Express API with `@zusammen/express`, MongoDB persistence, RabbitMQ transport, default wire format.
- `src/shipping.ts`: plain amqplib consumer bound to the `zusammen.events` exchange; stores processed message IDs in the same MongoDB transaction as the shipment.

## Run

From the repository root:

```sh
docker compose up -d        # MongoDB replica set + RabbitMQ
pnpm install
pnpm build                  # the sample uses the built packages

pnpm --filter @zusammen/sample-node-to-node shipping    # terminal 1
pnpm --filter @zusammen/sample-node-to-node api         # terminal 2

curl -X POST localhost:3000/orders -H 'content-type: application/json' -d '{"id":"o1","total":42}'
```

The worker prints `Shipping order o1`. Posting the same order again returns 409: the error response rolls the session back, so no event is published.

Things to try:

- Stop the worker, place orders, start it again: the events wait in its queue.
- RabbitMQ management UI at <http://localhost:15672> (guest/guest): see `orders-api.control`, its delay queues and `zusammen.events`.
