// A real NServiceBus endpoint for compatibility tests: the Node.js tests start it, send and publish messages with
// Zusammen, and assert on the JSON lines it writes (prefixed with "ZUSAMMEN ") for every handled or failed message.
using System.Text.Json;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using MongoDB.Driver;
using NServiceBus;
using Zusammen.NServiceBus;

var options = ParseArguments(args);

var configuration = new EndpointConfiguration(options["endpoint"]);
configuration.UseSerialization<SystemJsonSerializer>();
configuration.EnableInstallers();
configuration.SendFailedMessagesTo($"{options["endpoint"]}.error");

var topology = options["topology"] switch
{
    "conventional" => RoutingTopology.Conventional(QueueType.Quorum),
    "direct" => RoutingTopology.Direct(QueueType.Quorum),
    var other => throw new ArgumentException($"Unknown topology '{other}'"),
};
configuration.UseTransport(new RabbitMQTransport(topology, options["amqp"])
{
    ManagementApiConfiguration = new ManagementApiConfiguration(options["management"]),
});

if (options.ContainsKey("zusammen"))
{
    configuration.EnableZusammen();
}

// Always configured: assembly scanning picks up the persistence's installers either way
var persistence = configuration.UsePersistence<MongoPersistence>();
persistence.MongoClient(new MongoClient(options["mongo"]));
persistence.DatabaseName(options["endpoint"].Replace('.', '_').Replace('-', '_'));
if (options.ContainsKey("outbox"))
{
    configuration.EnableOutbox();
}

var recoverability = configuration.Recoverability();
recoverability.Immediate(settings => settings.NumberOfRetries(0));
recoverability.Delayed(settings => settings.NumberOfRetries(0));
recoverability.Failed(settings => settings.OnMessageSentToErrorQueue((message, _) =>
{
    Report.Write(new { @event = "failed", messageId = message.MessageId, error = message.Exception.Message });
    return Task.CompletedTask;
}));

var builder = Host.CreateApplicationBuilder();
builder.Logging.ClearProviders();
builder.Services.AddNServiceBusEndpoint(configuration);
using var host = builder.Build();
await host.StartAsync();
Report.Write(new { @event = "ready" });

// Runs until the test closes stdin
await Console.In.ReadToEndAsync();
await host.StopAsync();

static Dictionary<string, string> ParseArguments(string[] args)
{
    var options = new Dictionary<string, string>(StringComparer.Ordinal);
    for (var i = 0; i < args.Length; i++)
    {
        var name = args[i].TrimStart('-');
        var hasValue = i + 1 < args.Length && !args[i + 1].StartsWith("--", StringComparison.Ordinal);
        options[name] = hasValue ? args[++i] : "true";
    }

    return options;
}

static class Report
{
    public static void Write(object value)
    {
        lock (Gate)
        {
            Console.Out.WriteLine("ZUSAMMEN " + JsonSerializer.Serialize(value));
            Console.Out.Flush();
        }
    }

    static readonly object Gate = new();
}

namespace Sales.Messages
{
    public class PlaceOrder : ICommand
    {
        public string OrderId { get; set; } = "";
        public List<OrderLine> Lines { get; set; } = [];
    }

    public class OrderLine
    {
        public string ProductId { get; set; } = "";
        public int Quantity { get; set; }
    }

    public class OrderPlaced : IEvent
    {
        public string OrderId { get; set; } = "";
    }

    public class PlaceOrderHandler : IHandleMessages<PlaceOrder>
    {
        public Task Handle(PlaceOrder message, IMessageHandlerContext context)
        {
            Report.Write(new { @event = "handled", type = typeof(PlaceOrder).FullName, context.MessageId, body = message, headers = context.MessageHeaders });
            return Task.CompletedTask;
        }
    }

    public class OrderPlacedHandler : IHandleMessages<OrderPlaced>
    {
        public Task Handle(OrderPlaced message, IMessageHandlerContext context)
        {
            Report.Write(new { @event = "handled", type = typeof(OrderPlaced).FullName, context.MessageId, body = message, headers = context.MessageHeaders });
            return Task.CompletedTask;
        }
    }
}
