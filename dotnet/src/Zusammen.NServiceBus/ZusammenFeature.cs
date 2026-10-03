using NServiceBus;
using NServiceBus.Features;
using NServiceBus.Unicast.Messages;

namespace Zusammen.NServiceBus;

sealed class ZusammenFeature : Feature
{
    protected override void Setup(FeatureConfigurationContext context)
    {
        var settings = context.Settings.GetOrDefault<ZusammenSettings>() ?? new ZusammenSettings();
        var registry = context.Settings.Get<MessageMetadataRegistry>();

        // The registry is complete once the endpoint starts, so the resolver is built lazily
        var resolver = new Lazy<MessageTypeResolver>(() =>
            new MessageTypeResolver(settings.Overrides, registry.GetAllMessages().Select(m => m.MessageType)));

        context.Pipeline.Register(
            "ZusammenMessageType",
            new ZusammenMessageTypeBehavior(() => resolver.Value),
            "Derives NServiceBus.EnclosedMessageTypes from zusammen.message-type for messages sent by Zusammen");
        context.RegisterStartupTask(new ValidateMessageTypes(resolver));
    }

    sealed class ValidateMessageTypes(Lazy<MessageTypeResolver> resolver) : FeatureStartupTask
    {
        protected override Task OnStart(IMessageSession session, CancellationToken cancellationToken = default)
        {
            var ambiguities = resolver.Value.Ambiguities();
            if (ambiguities.Count > 0)
            {
                var details = string.Join("; ", ambiguities.Select(types => string.Join(", ", types.Select(t => t.FullName))));
                throw new InvalidOperationException(
                    $"Message types share a simple name, so Zusammen messages can't be matched to them unambiguously: {details}. Map them explicitly with {nameof(ZusammenSettings)}.{nameof(ZusammenSettings.MapMessageType)}.");
            }

            return Task.CompletedTask;
        }

        protected override Task OnStop(IMessageSession session, CancellationToken cancellationToken = default) => Task.CompletedTask;
    }
}
