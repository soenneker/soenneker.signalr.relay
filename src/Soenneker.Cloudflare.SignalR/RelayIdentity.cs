using System.Buffers;
using System.Security.Claims;
using MessagePack;
using Microsoft.Azure.SignalR.Protocol;

namespace Soenneker.Cloudflare.SignalR;

internal sealed class RelayIdentity(string token, Claim[] claims, CancellationTokenSource lifetime)
{
    private readonly Lock _gate = new();
    private Claim[] _claims = claims;
    public string Token { get; } = token;

    public bool Matches(ClaimsPrincipal principal)
    {
        var claims = principal.Claims.Where(c => c.Type is not ("aud" or "exp" or "iat" or "nbf" or "iss")).ToArray();
        lock (_gate) return Authorization(_claims).SequenceEqual(Authorization(claims));
    }

    public AckMessage Read(int ackId)
    {
        lock (_gate) return Acknowledge(ackId);
    }

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

    public void SetExpiration(DateTimeOffset expiration)
    {
        var delay = expiration - DateTimeOffset.UtcNow;
        // Reconnect before the platform's maximum timer interval, even for a longer-lived identity.
        lifetime.CancelAfter(TimeSpan.FromMilliseconds(Math.Clamp(delay.TotalMilliseconds, 0, int.MaxValue)));
    }

    private AckMessage Acknowledge(int ackId)
    {
        var buffer = new ArrayBufferWriter<byte>();
        var writer = new MessagePackWriter(buffer);
        writer.WriteMapHeader(_claims.Length);
        foreach (var claim in _claims)
        {
            writer.Write(claim.Type);
            writer.Write(claim.Value);
        }
        writer.Flush();
        return new AckMessage(ackId, 1) { Payload = new ReadOnlySequence<byte>(buffer.WrittenMemory) };
    }

    private static IEnumerable<(string Type, string Value)> Authorization(Claim[] values) =>
        values.Where(c => c.Type is not ("asrs.s.aeo" or "asrs.u.exp" or "asrs.u.iat" or "asrs.u.nbf" or "asrs.u.jti"))
            .Select(c => (c.Type, c.Value)).OrderBy(c => c.Type, StringComparer.Ordinal).ThenBy(c => c.Value, StringComparer.Ordinal);
}
