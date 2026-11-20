namespace Soenneker.Cloudflare.SignalR;

internal sealed record PendingInvocation(RelayPeer Caller, string CallerId, DateTimeOffset Deadline, RelayClient Receiver)
{
    public int RecoveryAttempts { get; set; }
    public byte[]? Reply { get; set; }
    public DateTimeOffset RetryAfter { get; set; }
}
