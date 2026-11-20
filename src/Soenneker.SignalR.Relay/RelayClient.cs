using Soenneker.Atomics.ValueInts;
using Soenneker.SignalR.Relay.Dtos;
using System.Collections.Concurrent;

namespace Soenneker.SignalR.Relay;

/// <summary>Holds routing, membership, and invocation state for an attached client.</summary>
/// <param name="Peer">Client transport.</param>
/// <param name="Server">Assigned server tunnel.</param>
/// <param name="Protocol">Negotiated hub protocol.</param>
/// <param name="UserId">Authenticated user identifier, when present.</param>
/// <param name="Identity">Session token and refreshable claims.</param>
internal sealed record RelayClient(RelayPeer Peer, RelayPeer Server, string Protocol, string? UserId, RelayIdentity Identity)
{
    /// <summary>Pending management completions keyed by invocation identifier.</summary>
    public Dictionary<string, TaskCompletionSource<byte[]>> ManagementResults { get; } = new(StringComparer.Ordinal);

    /// <summary>Management results retained by idempotency key for repeated requests.</summary>
    public Dictionary<Guid, CachedClientResult> RetryableResults { get; } = [];

    /// <summary>Number of server and management invocations awaiting completion or cleanup.</summary>
    private ValueAtomicInt _outstandingInvocations;

    /// <summary>Gets whether a client completion could match an outstanding invocation.</summary>
    public bool HasOutstandingInvocations => _outstandingInvocations.Read() > 0;

    /// <summary>Counts a newly registered invocation before sending it to the client.</summary>
    public void TrackInvocation() => _outstandingInvocations.Increment();

    /// <summary>Releases the count for a completed or retired invocation.</summary>
    public void ReleaseInvocation() => _outstandingInvocations.Decrement();

    /// <summary>Membership by group name. Zero denotes a connection grant and one denotes an inherited user grant.</summary>
    public ConcurrentDictionary<string, byte> Groups { get; } = new(StringComparer.Ordinal);
}
