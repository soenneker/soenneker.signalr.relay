using System.Collections.Concurrent;

namespace Soenneker.Cloudflare.SignalR;

internal sealed class RelayDirectory : IRelayDirectory
{
    private readonly ConcurrentDictionary<string, RelayHub> _hubs = new(StringComparer.Ordinal);
    private readonly ConcurrentDictionary<string, (string Hub, RelayIdentity Identity)> _sessions = new(StringComparer.Ordinal);

    public bool TryBind(string hub, RelayIdentity identity) =>
        identity.Token.Length == 0 || _sessions.TryAdd(identity.Token, (hub, identity));

    public bool IsBound(string token) => _sessions.ContainsKey(token);

    public bool CanResume(string hub, string token, System.Security.Claims.ClaimsPrincipal principal) =>
        !_sessions.TryGetValue(token, out var current) || (current.Hub == hub && current.Identity.Matches(principal));

    public void Unbind(RelayIdentity identity)
    {
        if (identity.Token.Length != 0) _sessions.TryRemove(identity.Token, out _);
    }

    public void Maintain(DateTimeOffset now)
    {
        foreach (var hub in _hubs.Values) hub.Maintain(now);
    }

    public RelayHub? Find(string name) => _hubs.GetValueOrDefault(name);
    public RelayHub Get(string name) => _hubs.GetOrAdd(name, static _ => new RelayHub());
}
