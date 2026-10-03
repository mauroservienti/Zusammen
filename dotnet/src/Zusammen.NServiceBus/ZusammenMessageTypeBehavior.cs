using NServiceBus;
using NServiceBus.Pipeline;

namespace Zusammen.NServiceBus;

/// <summary>
/// Runs before deserialization: derives a missing <c>NServiceBus.EnclosedMessageTypes</c> header from
/// <c>zusammen.message-type</c>. Messages that already carry the header are left untouched.
/// </summary>
sealed class ZusammenMessageTypeBehavior(Func<MessageTypeResolver> resolver) : Behavior<IIncomingPhysicalMessageContext>
{
    public override Task Invoke(IIncomingPhysicalMessageContext context, Func<Task> next)
    {
        var headers = context.Message.Headers;
        if (!headers.ContainsKey(Headers.EnclosedMessageTypes) &&
            headers.TryGetValue(ZusammenHeaders.MessageType, out var zusammenMessageType))
        {
            headers[Headers.EnclosedMessageTypes] = resolver().Resolve(zusammenMessageType).FullName!;
        }

        return next();
    }
}
