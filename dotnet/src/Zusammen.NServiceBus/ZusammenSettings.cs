namespace Zusammen.NServiceBus;

/// <summary>
/// Settings for receiving Zusammen messages.
/// </summary>
public sealed class ZusammenSettings
{
    internal Dictionary<string, Type> Overrides { get; } = new(StringComparer.Ordinal);

    /// <summary>
    /// Maps a Zusammen message type name to a .NET type, overriding the match by simple type name. Use it when type
    /// names are ambiguous, or differ between Node.js and .NET.
    /// </summary>
    public ZusammenSettings MapMessageType(string zusammenMessageType, Type messageType)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(zusammenMessageType);
        ArgumentNullException.ThrowIfNull(messageType);
        Overrides[zusammenMessageType] = messageType;
        return this;
    }

    /// <inheritdoc cref="MapMessageType(string, Type)"/>
    public ZusammenSettings MapMessageType<TMessage>(string zusammenMessageType) =>
        MapMessageType(zusammenMessageType, typeof(TMessage));
}
