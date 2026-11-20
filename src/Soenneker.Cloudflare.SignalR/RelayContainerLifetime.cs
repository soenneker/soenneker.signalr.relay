using System.Runtime.InteropServices;

namespace Soenneker.Cloudflare.SignalR;

// ConsoleLifetime calls StopApplication immediately on SIGTERM, which closes the SignalR dispatcher.
// Begin the drain before notifying the host so existing HTTP transports stay available.
internal sealed class RelayContainerLifetime(IRelayDrain drain, IHostApplicationLifetime lifetime) : IHostLifetime, IDisposable
{
    private readonly List<PosixSignalRegistration> _signals = [];
    private int _stopping;

    public Task WaitForStartAsync(CancellationToken cancellationToken)
    {
        foreach (var signal in new[] { PosixSignal.SIGTERM, PosixSignal.SIGINT, PosixSignal.SIGQUIT })
            _signals.Add(PosixSignalRegistration.Create(signal, OnSignal));
        return Task.CompletedTask;
    }

    private void OnSignal(PosixSignalContext context)
    {
        context.Cancel = true;
        if (Interlocked.Exchange(ref _stopping, 1) == 0) _ = DrainThenStop();
    }

    private async Task DrainThenStop()
    {
        try { await drain.BeginAsync(CancellationToken.None); }
        finally { lifetime.StopApplication(); }
    }

    public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;
    public void Dispose()
    {
        foreach (var signal in _signals) signal.Dispose();
    }
}
