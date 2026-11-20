namespace Soenneker.Cloudflare.SignalR;

internal sealed class RelayDrainLease(Action release) : IDisposable
{
    private Action? _release = release;
    public void Dispose() => Interlocked.Exchange(ref _release, null)?.Invoke();
}
