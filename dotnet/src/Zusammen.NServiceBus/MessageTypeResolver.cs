namespace Zusammen.NServiceBus;

/// <summary>
/// Resolves Zusammen message type names: explicit overrides first, then the endpoint's message types by FullName or
/// simple name.
/// </summary>
sealed class MessageTypeResolver
{
    public MessageTypeResolver(IReadOnlyDictionary<string, Type> overrides, IEnumerable<Type> messageTypes)
    {
        this.overrides = overrides;
        var types = messageTypes.Distinct().ToList();
        byFullName = types.Where(t => t.FullName is not null).ToDictionary(t => t.FullName!, StringComparer.Ordinal);
        bySimpleName = types.GroupBy(t => t.Name, StringComparer.Ordinal).ToDictionary(g => g.Key, g => g.ToList(), StringComparer.Ordinal);
    }

    /// <summary>
    /// Message types sharing a simple name, without an override to disambiguate them.
    /// </summary>
    public IReadOnlyList<IReadOnlyList<Type>> Ambiguities() =>
        bySimpleName
            .Where(g => g.Value.Count > 1 && !overrides.ContainsKey(g.Key))
            .Select(g => (IReadOnlyList<Type>)g.Value)
            .ToList();

    public Type Resolve(string zusammenMessageType)
    {
        if (overrides.TryGetValue(zusammenMessageType, out var mapped))
        {
            return mapped;
        }

        if (byFullName.TryGetValue(zusammenMessageType, out var byFull))
        {
            return byFull;
        }

        if (bySimpleName.TryGetValue(zusammenMessageType, out var candidates))
        {
            if (candidates.Count == 1)
            {
                return candidates[0];
            }

            throw new InvalidOperationException(
                $"Zusammen message type '{zusammenMessageType}' matches several message types ({string.Join(", ", candidates.Select(t => t.FullName))}). Map it explicitly with {nameof(ZusammenSettings)}.{nameof(ZusammenSettings.MapMessageType)}.");
        }

        throw new InvalidOperationException(
            $"Zusammen message type '{zusammenMessageType}' doesn't match any message type known to this endpoint. Map it explicitly with {nameof(ZusammenSettings)}.{nameof(ZusammenSettings.MapMessageType)}, or map the .NET type on the sending side.");
    }

    readonly IReadOnlyDictionary<string, Type> overrides;
    readonly Dictionary<string, Type> byFullName;
    readonly Dictionary<string, List<Type>> bySimpleName;
}
