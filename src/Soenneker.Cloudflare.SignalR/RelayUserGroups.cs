namespace Soenneker.Cloudflare.SignalR;

// Accessed under the hub routing lock. The expiry index retains one entry per grant, including after renewal.
internal sealed class RelayUserGroups(RelayAudienceIndex users, RelayAudienceIndex groups)
{
    private readonly Dictionary<string, Dictionary<string, (DateTimeOffset End, bool DefaultTtl)>> _grants = new(StringComparer.Ordinal);
    private readonly SortedSet<(DateTimeOffset End, string User, string Group)> _expires = new(
        Comparer<(DateTimeOffset End, string User, string Group)>.Create((x, y) =>
        {
            int result = x.End.CompareTo(y.End);
            if (result == 0) result = string.CompareOrdinal(x.User, y.User);
            return result != 0 ? result : string.CompareOrdinal(x.Group, y.Group);
        }));

    public bool Join(string user, string group, int? ttl)
    {
        if (ttl < 0) return false;
        var now = DateTimeOffset.UtcNow;
        Expire(now);
        if (ttl != 0)
        {
            if (_expires.Count >= 65536 && !Contains(user, group)) return false;
            if (!_grants.TryGetValue(user, out var grants)) _grants[user] = grants = new(StringComparer.Ordinal);
            if (grants.TryGetValue(group, out var old)) _expires.Remove((old.End, user, group));
            else if (ttl is null && grants.Count(p => p.Value.DefaultTtl) >= 100)
            {
                var oldest = grants.Where(p => p.Value.DefaultTtl).MinBy(p => p.Value.End);
                Remove(user, oldest.Key, inheritedOnly: true);
            }
            var end = ttl.HasValue ? now.AddSeconds(ttl.Value) : now.AddYears(1);
            grants[group] = (end, ttl is null);
            _expires.Add((end, user, group));
        }
        foreach (var client in users.Members(user))
        {
            groups.Add(group, client);
            if (ttl == 0) client.Groups[group] = 0;
            else client.Groups.TryAdd(group, 1); // Preserve an independent connection-level grant.
        }
        return true;
    }

    public bool Contains(string user, string group) => _grants.TryGetValue(user, out var grants) && grants.ContainsKey(group);

    public void Attach(RelayClient client, string user)
    {
        Expire(DateTimeOffset.UtcNow);
        if (!_grants.TryGetValue(user, out var grants)) return;
        foreach (var group in grants.Keys)
        {
            groups.Add(group, client);
            client.Groups.TryAdd(group, 1);
        }
    }

    public void Leave(string user, string? group)
    {
        if (group is not null) { Remove(user, group, false); return; }
        if (_grants.TryGetValue(user, out var grants))
            foreach (var name in grants.Keys.ToArray()) Remove(user, name, false);
        foreach (var client in users.Members(user))
            foreach (var name in client.Groups.Keys)
            {
                groups.Remove(name, client);
                client.Groups.TryRemove(name, out _);
            }
    }

    public void Expire(DateTimeOffset now)
    {
        while (_expires.Count > 0 && _expires.Min.End <= now)
        {
            var entry = _expires.Min;
            Remove(entry.User, entry.Group, true);
        }
    }

    private void Remove(string user, string group, bool inheritedOnly)
    {
        if (_grants.TryGetValue(user, out var grants) && grants.Remove(group, out var grant))
        {
            _expires.Remove((grant.End, user, group));
            if (grants.Count == 0) _grants.Remove(user);
        }
        foreach (var client in users.Members(user))
        {
            if (inheritedOnly && client.Groups.GetValueOrDefault(group) != 1) continue;
            groups.Remove(group, client);
            client.Groups.TryRemove(group, out _);
        }
    }
}
