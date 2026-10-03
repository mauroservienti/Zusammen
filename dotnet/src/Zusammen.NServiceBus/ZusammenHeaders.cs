namespace Zusammen.NServiceBus;

/// <summary>
/// Headers set by Zusammen.
/// </summary>
public static class ZusammenHeaders
{
    /// <summary>
    /// The message type name chosen on the Node.js side: the .NET FullName when mapped there, otherwise the class name.
    /// </summary>
    public const string MessageType = "zusammen.message-type";
}
