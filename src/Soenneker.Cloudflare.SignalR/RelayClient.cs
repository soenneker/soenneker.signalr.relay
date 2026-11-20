using System.Collections.Concurrent;

namespace Soenneker.Cloudflare.SignalR;

internal sealed record RelayClient(RelayPeer Peer, RelayPeer Server, string Protocol, string? UserId, RelayIdentity Identity)
{
    public Dictionary<string, TaskCompletionSource<byte[]>> ManagementResults { get; } = new(StringComparer.Ordinal);
    public Dictionary<Guid, CachedClientResult> RetryableResults { get; } = [];
    public int OutstandingInvocations;
    public ConcurrentDictionary<string, byte> Groups { get; } = new(StringComparer.Ordinal);
}
