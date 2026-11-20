using System.Buffers;
using System.Security.Claims;
using MessagePack;
using Microsoft.Azure.SignalR.Protocol;

namespace Soenneker.SignalR.Relay;

/// <summary>Maintains a connection's session token, claims, and authentication expiry.</summary>
/// <param name="token">Session token.</param>
/// <param name="claims">Initial connection claims.</param>
/// <param name="lifetime">Cancellation source that enforces connection expiration.</param>
internal sealed class RelayIdentity(string token, Claim[] claims, CancellationTokenSource lifetime)
{
    /// <summary>Serializes claim reads and refreshes for this identity.</summary>
    private readonly Lock _gate = new();

    /// <summary>Current claims reported to the server tunnel.</summary>
    private Claim[] _claims = claims;

    /// <summary>Gets the token used to look up or resume this connection.</summary>
    public string Token { get; } = token;

    /// <summary>Compares the principal's authorization claims with the current session claims.</summary>
    /// <param name="principal">Principal presenting the new token.</param>
    /// <returns>True if the authorization claims match.</returns>
    public bool Matches(ClaimsPrincipal principal)
    {
        var claims = principal.Claims.Where(c => c.Type is not ("aud" or "exp" or "iat" or "nbf" or "iss")).ToArray();
        lock (_gate) return Authorization(_claims).SequenceEqual(Authorization(claims));
    }

    /// <summary>Encodes the current claims for a service claim lookup.</summary>
    /// <param name="ackId">Identifier to echo in the service acknowledgement.</param>
    /// <returns>An acknowledgement containing the current claims.</returns>
    public AckMessage Read(int ackId)
    {
        lock (_gate) return Acknowledge(ackId);
    }

    /// <summary>Applies a valid refresh and informs the server, or aborts the client on rejection.</summary>
    /// <param name="request">Requested replacement claims and expiration.</param>
    /// <param name="client">Attached client whose state is being updated.</param>
    /// <returns>An acknowledgement carrying the updated claims on success or a failure status on rejection.</returns>
    public AckMessage Refresh(RefreshAuthMessage request, RelayClient client)
    {
        lock (_gate)
        {
            var updated = request.Claims ?? [];
            if (lifetime.IsCancellationRequested || request.ExpireTime <= DateTimeOffset.UtcNow ||
                !Authorization(_claims).SequenceEqual(Authorization(updated)))
            {
                client.Peer.Abort();
                return new AckMessage(request.AckId, 5);
            }
            _claims = updated;
            SetExpiration(request.ExpireTime);
            client.Server.Send(new UpdateConnectionClaimsMessage(client.Peer.Id, updated));
            return Acknowledge(request.AckId);
        }
    }

    /// <summary>Schedules connection cancellation at the supplied expiration time.</summary>
    /// <param name="expiration">Time at which the session credentials expire.</param>
    public void SetExpiration(DateTimeOffset expiration)
    {
        var delay = expiration - DateTimeOffset.UtcNow;
        // Reconnect before the platform's maximum timer interval, even for a longer-lived identity.
        lifetime.CancelAfter(TimeSpan.FromMilliseconds(Math.Clamp(delay.TotalMilliseconds, 0, int.MaxValue)));
    }

    /// <summary>Builds a successful service acknowledgement containing the current claims.</summary>
    /// <param name="ackId">Identifier to echo in the service acknowledgement.</param>
    /// <returns>An acknowledgement correlated with the supplied identifier.</returns>
    private AckMessage Acknowledge(int ackId)
    {
        using var writer = new MessagePackWriter();
        writer.WriteMapHeader(_claims.Length);
        foreach (var claim in _claims)
        {
            writer.WriteString(claim.Type);
            writer.WriteString(claim.Value);
        }
        return new AckMessage(ackId, 1) { Payload = new ReadOnlySequence<byte>(writer.ToArray()) };
    }

    /// <summary>Orders authorization-relevant claims for comparison across token refreshes.</summary>
    /// <param name="values">Claims to filter and order.</param>
    /// <returns>Claim type and value pairs sorted in ordinal order.</returns>
    private static IEnumerable<(string Type, string Value)> Authorization(Claim[] values) =>
        values.Where(c => c.Type is not ("asrs.s.aeo" or "asrs.u.exp" or "asrs.u.iat" or "asrs.u.nbf" or "asrs.u.jti"))
            .Select(c => (c.Type, c.Value)).OrderBy(c => c.Type, StringComparer.Ordinal).ThenBy(c => c.Value, StringComparer.Ordinal);
}
