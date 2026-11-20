namespace Soenneker.SignalR.Relay;

/// <summary>Tracks a server invocation until the caller receives its reply or recovery ends.</summary>
/// <param name="Caller">Tunnel serving the caller.</param>
/// <param name="CallerId">Calling server identifier carried in completion messages.</param>
/// <param name="Deadline">Time at which to abandon the invocation.</param>
/// <param name="Receiver">Client selected to execute the invocation.</param>
internal sealed record PendingInvocation(RelayPeer Caller, string CallerId, DateTimeOffset Deadline, RelayClient Receiver)
{
    /// <summary>Number of times the captured completion has been resent.</summary>
    public int RecoveryAttempts { get; set; }

    /// <summary>Original completion frame retained for recovery, if one has arrived.</summary>
    public byte[]? Reply { get; set; }

    /// <summary>Earliest time maintenance may resend the retained reply.</summary>
    public DateTimeOffset RetryAfter { get; set; }
}
