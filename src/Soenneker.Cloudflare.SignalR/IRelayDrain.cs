namespace Soenneker.Cloudflare.SignalR;

internal interface IRelayDrain
{
    bool IsDraining { get; }
    IDisposable? AdmitClient();
    Task BeginAsync(CancellationToken cancellationToken);
}
