// The Billing endpoint: an ordinary NServiceBus endpoint, plus EnableZusammen() for messages Zusammen sends
// without a .NET type mapping.
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using NServiceBus;
using Zusammen.NServiceBus;

var builder = Host.CreateApplicationBuilder(args);

var endpointConfiguration = new EndpointConfiguration("Billing");
endpointConfiguration.UseSerialization<SystemJsonSerializer>();
endpointConfiguration.UseTransport(new RabbitMQTransport(
    RoutingTopology.Conventional(QueueType.Quorum),
    builder.Configuration["RabbitMQ"] ?? "host=localhost"));
endpointConfiguration.EnableInstallers();
endpointConfiguration.EnableZusammen();

builder.Services.AddNServiceBusEndpoint(endpointConfiguration);
await builder.Build().RunAsync();

namespace Sales.Messages
{
    // Mapped on the Node.js side: arrives with NServiceBus.EnclosedMessageTypes
    public class OrderPlaced : IEvent
    {
        public string OrderId { get; set; } = "";
    }
}

namespace Billing.Messages
{
    // Not mapped on the Node.js side: resolved from zusammen.message-type ("ChargeCustomer") by simple name
    public class ChargeCustomer : ICommand
    {
        public string OrderId { get; set; } = "";
        public decimal Amount { get; set; }
    }
}

namespace Billing
{
    using Billing.Messages;
    using Sales.Messages;

    public class OrderPlacedHandler(ILogger<OrderPlacedHandler> logger) : IHandleMessages<OrderPlaced>
    {
        public Task Handle(OrderPlaced message, IMessageHandlerContext context)
        {
            logger.LogInformation("Order {OrderId} placed, preparing invoice", message.OrderId);
            return Task.CompletedTask;
        }
    }

    public class ChargeCustomerHandler(ILogger<ChargeCustomerHandler> logger) : IHandleMessages<ChargeCustomer>
    {
        public Task Handle(ChargeCustomer message, IMessageHandlerContext context)
        {
            logger.LogInformation("Charging {Amount} for order {OrderId} (message {MessageId})", message.Amount, message.OrderId, context.MessageId);
            return Task.CompletedTask;
        }
    }
}
