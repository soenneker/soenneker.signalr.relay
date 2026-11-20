namespace Soenneker.Cloudflare.SignalR;

internal sealed class RelayOutputBudget(long limit) : IRelayOutputBudget
{
    private readonly HashSet<RelayPeer> _clients = [];
    public object Gate { get; } = new();
    internal long Used { get; private set; }

    // All accounting and queue admission run under Gate. Keep an eighth for API tunnel control traffic.
    public bool TryReserve(RelayPeer peer, int bytes, bool serverControl, out List<RelayPeer>? evicted)
    {
        evicted = null;
        if (bytes == 0) return true;
        long ceiling = serverControl ? limit : limit - limit / 8;
        if (bytes > ceiling) return false;
        while (Used > ceiling - bytes)
        {
            RelayPeer? victim = null;
            foreach (var candidate in _clients)
            {
                if (!candidate.HasQueuedData) continue;
                if (victim is null || candidate.PendingBytes > victim.PendingBytes) victim = candidate;
            }
            if (victim is null || victim == peer) return false;
            if (peer.ServerId is null && victim.PendingBytes <= peer.PendingBytes) return false;
            victim.CloseQueue();
            (evicted ??= []).Add(victim);
            RelayMetrics.ShedClients.Add(1);
        }
        Used += bytes;
        if (peer.ServerId is null) _clients.Add(peer);
        RelayMetrics.QueuedBytes.Add(bytes);
        return true;
    }

    public void Release(RelayPeer peer, int bytes)
    {
        Used -= bytes;
        if (peer.PendingBytes == 0) _clients.Remove(peer);
        RelayMetrics.QueuedBytes.Add(-bytes);
    }
}
