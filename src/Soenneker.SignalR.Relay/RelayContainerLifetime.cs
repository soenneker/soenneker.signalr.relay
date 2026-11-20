using Soenneker.Atomics.ValueBools;
using System.Runtime.InteropServices;

using Soenneker.SignalR.Relay.Abstract;

namespace Soenneker.SignalR.Relay;

// ConsoleLifetime calls StopApplication immediately on SIGTERM, which closes the SignalR dispatcher.
// Begin the drain before notifying the host so existing HTTP transports stay available.
/// <summary>Drains clients on container termination before asking the host to stop.</summary>
/// <param name="drain">Coordinator for client admission and shutdown.</param>
/// <param name="lifetime">Host lifetime used to observe or request shutdown.</param>
internal sealed class RelayContainerLifetime(IRelayDrain drain, IHostApplicationLifetime lifetime) : IHostLifetime, IDisposable
{
    /// <summary>Signal registrations released when the host lifetime is disposed.</summary>
    private readonly List<PosixSignalRegistration> _signals = [];

    /// <summary>Set by the first termination signal to start a single drain.</summary>
    private ValueAtomicBool _stopping;

    public Task WaitForStartAsync(CancellationToken cancellationToken)
    {
        foreach (var signal in new[] { PosixSignal.SIGTERM, PosixSignal.SIGINT, PosixSignal.SIGQUIT })
            _signals.Add(PosixSignalRegistration.Create(signal, OnSignal));
        return Task.CompletedTask;
    }

    /// <summary>Suppresses default termination and schedules the drain once.</summary>
    /// <param name="context">Termination signal delivered by the container.</param>
    private void OnSignal(PosixSignalContext context)
    {
        context.Cancel = true;
        if (_stopping.TrySetTrue()) _ = DrainThenStop();
    }

    /// <summary>Waits for the drain window and requests host shutdown even if draining fails.</summary>
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
