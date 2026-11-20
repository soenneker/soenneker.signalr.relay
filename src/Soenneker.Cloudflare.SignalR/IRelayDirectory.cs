namespace Soenneker.Cloudflare.SignalR;

internal interface IRelayDirectory
{
    bool TryBind(string hub, RelayIdentity identity);
    bool IsBound(string token);
    bool CanResume(string hub, string token, System.Security.Claims.ClaimsPrincipal principal);
    void Unbind(RelayIdentity identity);
    void Maintain(DateTimeOffset now);
    RelayHub? Find(string name);
    RelayHub Get(string name);
}
