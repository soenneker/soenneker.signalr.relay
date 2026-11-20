namespace Soenneker.Cloudflare.SignalR;

// The owning hub serializes membership changes and recipient snapshots under its routing lock.
internal sealed class RelayAudienceIndex
{
    private readonly Dictionary<string, HashSet<RelayClient>> _members = new(StringComparer.Ordinal);

    public void Add(string name, RelayClient client)
    {
        if (!_members.TryGetValue(name, out var set))
            _members[name] = set = [];
        set.Add(client);
    }

    public void Remove(string name, RelayClient client)
    {
        if (!_members.TryGetValue(name, out var set)) return;
        set.Remove(client);
        if (set.Count == 0) _members.Remove(name);
    }

    public bool Contains(string name) => _members.ContainsKey(name);

    public IEnumerable<RelayClient> Members(string name) =>
        _members.TryGetValue(name, out var set) ? set : [];
}
