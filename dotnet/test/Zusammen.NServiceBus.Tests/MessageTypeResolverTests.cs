using NServiceBus;
using NServiceBus.Testing;

namespace Zusammen.NServiceBus.Tests;

public class MessageTypeResolverTests
{
    [Test]
    public void Resolves_by_simple_name()
    {
        var resolver = new MessageTypeResolver(new Dictionary<string, Type>(), [typeof(Sales.OrderPlaced), typeof(Sales.PlaceOrder)]);

        Assert.That(resolver.Resolve("OrderPlaced"), Is.EqualTo(typeof(Sales.OrderPlaced)));
    }

    [Test]
    public void Resolves_by_full_name()
    {
        var resolver = new MessageTypeResolver(new Dictionary<string, Type>(), [typeof(Sales.OrderPlaced), typeof(Billing.OrderPlaced)]);

        Assert.That(resolver.Resolve(typeof(Billing.OrderPlaced).FullName!), Is.EqualTo(typeof(Billing.OrderPlaced)));
    }

    [Test]
    public void Overrides_win()
    {
        var overrides = new Dictionary<string, Type> { ["OrderPlaced"] = typeof(Billing.OrderPlaced) };
        var resolver = new MessageTypeResolver(overrides, [typeof(Sales.OrderPlaced), typeof(Billing.OrderPlaced)]);

        Assert.That(resolver.Resolve("OrderPlaced"), Is.EqualTo(typeof(Billing.OrderPlaced)));
        Assert.That(resolver.Ambiguities(), Is.Empty);
    }

    [Test]
    public void Reports_ambiguous_simple_names()
    {
        var resolver = new MessageTypeResolver(new Dictionary<string, Type>(), [typeof(Sales.OrderPlaced), typeof(Billing.OrderPlaced), typeof(Sales.PlaceOrder)]);

        Assert.That(resolver.Ambiguities(), Has.Count.EqualTo(1));
        Assert.That(() => resolver.Resolve("OrderPlaced"), Throws.InvalidOperationException.With.Message.Contains("matches several message types"));
    }

    [Test]
    public void Unknown_names_fail()
    {
        var resolver = new MessageTypeResolver(new Dictionary<string, Type>(), [typeof(Sales.OrderPlaced)]);

        Assert.That(() => resolver.Resolve("Missing"), Throws.InvalidOperationException.With.Message.Contains("doesn't match any message type"));
    }
}

public class ZusammenMessageTypeBehaviorTests
{
    static readonly MessageTypeResolver Resolver = new(new Dictionary<string, Type>(), [typeof(Sales.OrderPlaced)]);

    [Test]
    public async Task Derives_EnclosedMessageTypes_from_zusammen_message_type()
    {
        var context = new TestableIncomingPhysicalMessageContext();
        context.Message.Headers[ZusammenHeaders.MessageType] = "OrderPlaced";

        await new ZusammenMessageTypeBehavior(() => Resolver).Invoke(context, () => Task.CompletedTask);

        Assert.That(context.Message.Headers[Headers.EnclosedMessageTypes], Is.EqualTo(typeof(Sales.OrderPlaced).FullName));
    }

    [Test]
    public async Task Leaves_existing_EnclosedMessageTypes_alone()
    {
        var context = new TestableIncomingPhysicalMessageContext();
        context.Message.Headers[ZusammenHeaders.MessageType] = "OrderPlaced";
        context.Message.Headers[Headers.EnclosedMessageTypes] = "Explicit.Type";

        await new ZusammenMessageTypeBehavior(() => Resolver).Invoke(context, () => Task.CompletedTask);

        Assert.That(context.Message.Headers[Headers.EnclosedMessageTypes], Is.EqualTo("Explicit.Type"));
    }

    [Test]
    public async Task Ignores_messages_not_sent_by_Zusammen()
    {
        var context = new TestableIncomingPhysicalMessageContext();
        context.Message.Headers.Remove(Headers.EnclosedMessageTypes);

        await new ZusammenMessageTypeBehavior(() => Resolver).Invoke(context, () => Task.CompletedTask);

        Assert.That(context.Message.Headers.ContainsKey(Headers.EnclosedMessageTypes), Is.False);
    }

    [Test]
    public void Unknown_types_fail_the_message()
    {
        var context = new TestableIncomingPhysicalMessageContext();
        context.Message.Headers[ZusammenHeaders.MessageType] = "Missing";

        Assert.That(() => new ZusammenMessageTypeBehavior(() => Resolver).Invoke(context, () => Task.CompletedTask), Throws.InvalidOperationException);
    }
}
