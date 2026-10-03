# Zusammen.NServiceBus

Lets [NServiceBus](https://particular.net/nservicebus) 10 endpoints consume messages sent by [Zusammen](https://github.com/mauroservienti/Zusammen) from Node.js without explicit .NET type mappings on the sending side.

```csharp
var endpointConfiguration = new EndpointConfiguration("Billing");
endpointConfiguration.EnableZusammen();
```

Messages Zusammen sends with a .NET type mapping carry `NServiceBus.EnclosedMessageTypes` and need nothing. Unmapped messages carry only `zusammen.message-type` (the Node.js class name). Before deserialization, the package sets `NServiceBus.EnclosedMessageTypes` from it, matching:

1. explicit mappings: `EnableZusammen().MapMessageType<ChargeCustomer>("ChargeCustomerCommand")`;
2. the endpoint's message types by FullName;
3. the endpoint's message types by simple name.

Message types sharing a simple name without an explicit mapping fail the endpoint at startup. A name that matches nothing fails the message, which goes through the endpoint's recoverability.

## Repository layout

- `src/Zusammen.NServiceBus`: the package.
- `test/Zusammen.NServiceBus.Tests`: unit tests (`dotnet test Zusammen.NServiceBus.slnx`).
- `compat/Zusammen.Compat.Endpoint`: the NServiceBus endpoint the Node.js compatibility tests run.
