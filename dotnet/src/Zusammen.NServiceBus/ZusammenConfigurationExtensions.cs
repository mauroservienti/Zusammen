using NServiceBus;
using NServiceBus.Configuration.AdvancedExtensibility;

namespace Zusammen.NServiceBus;

/// <summary>
/// Configuration for receiving messages sent by Zusammen.
/// </summary>
public static class ZusammenConfigurationExtensions
{
    /// <summary>
    /// Lets the endpoint consume Zusammen messages that carry only <c>zusammen.message-type</c>, matching them to the
    /// endpoint's message types by simple name. Messages with <c>NServiceBus.EnclosedMessageTypes</c> are unaffected.
    /// </summary>
    public static ZusammenSettings EnableZusammen(this EndpointConfiguration endpointConfiguration)
    {
        ArgumentNullException.ThrowIfNull(endpointConfiguration);
        endpointConfiguration.EnableFeature<ZusammenFeature>();
        return endpointConfiguration.GetSettings().GetOrCreate<ZusammenSettings>();
    }
}
