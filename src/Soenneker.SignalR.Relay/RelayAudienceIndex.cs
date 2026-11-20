namespace Soenneker.SignalR.Relay;

// The owning hub serializes membership changes and recipient snapshots under its routing lock.
/// <summary>Indexes a hub's client connections by audience name.</summary>
internal sealed class RelayAudienceIndex
{
    /// <summary>Connections indexed by group name or user identifier.</summary>
    private readonly Dictionary<string, HashSet<RelayClient>> _members = new(StringComparer.Ordinal);

    /// <summary>Adds a connection to the named audience.</summary>
    /// <param name="name">Audience name.</param>
    /// <param name="client">Attached client whose state is being updated.</param>
    public void Add(string name, RelayClient client)
    {
        if (!_members.TryGetValue(name, out var set))
            _members[name] = set = [];
        set.Add(client);
    }

    /// <summary>Removes a connection and drops the audience entry when its last member leaves.</summary>
    /// <param name="name">Audience name.</param>
    /// <param name="client">Attached client whose state is being updated.</param>
    public void Remove(string name, RelayClient client)
    {
        if (!_members.TryGetValue(name, out var set)) return;
        set.Remove(client);
        if (set.Count == 0) _members.Remove(name);
    }

    /// <summary>Checks whether the audience has any members.</summary>
    /// <param name="name">Audience name.</param>
    /// <returns>True when the named audience contains at least one connection.</returns>
    public bool Contains(string name) => _members.ContainsKey(name);

    /// <summary>Enumerates the audience's connections under the owning hub's routing lock.</summary>
    /// <param name="name">Audience name.</param>
    /// <returns>The indexed connections, or an empty sequence for an unknown audience.</returns>
    public IEnumerable<RelayClient> Members(string name) =>
        _members.TryGetValue(name, out var set) ? set : [];
}
