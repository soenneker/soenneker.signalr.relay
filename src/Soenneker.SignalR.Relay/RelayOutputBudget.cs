using Soenneker.SignalR.Relay.Abstract;

namespace Soenneker.SignalR.Relay;

/// <summary>Enforces the shared output byte limit and reserves capacity for tunnel control traffic.</summary>
internal sealed class RelayOutputBudget : IRelayOutputBudget
{
    /// <summary>Total output queue capacity in bytes.</summary>
    private readonly long _limit;

    /// <summary>Reads and validates the byte limit shared by all relay peers.</summary>
    /// <param name="configuration">Configuration supplied by the relay host.</param>
    public RelayOutputBudget(IConfiguration configuration)
    {
        _limit = configuration.GetValue<long?>("Relay:MaxQueuedBytes") ?? 64L * 1024 * 1024;
        if (_limit < 4 * 1024 * 1024 || _limit > int.MaxValue)
            throw new InvalidOperationException("Relay:MaxQueuedBytes must be between 4 MiB and 2 GiB minus one byte.");
    }

    /// <summary>Creates a budget with an explicit byte limit for isolated queues.</summary>
    /// <param name="limit">Total capacity in bytes.</param>
    internal RelayOutputBudget(long limit) => _limit = limit;

    /// <summary>Client peers eligible for eviction while they have queued data.</summary>
    private readonly HashSet<RelayPeer> _clients = [];
    public object Gate { get; } = new();

    /// <summary>Gets the number of bytes currently reserved across peers.</summary>
    internal long Used { get; private set; }

    // All accounting and queue admission run under Gate. Keep an eighth for API tunnel control traffic.
    public bool TryReserve(RelayPeer peer, int bytes, bool serverControl, out List<RelayPeer>? evicted)
    {
        evicted = null;
        if (bytes == 0) return true;
        long ceiling = serverControl ? _limit : _limit - _limit / 8;
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
