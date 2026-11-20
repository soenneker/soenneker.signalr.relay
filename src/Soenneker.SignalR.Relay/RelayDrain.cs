using Soenneker.Atomics.ValueBools;
using Soenneker.Asyncs.Locks;
using Soenneker.SignalR.Relay.Abstract;

namespace Soenneker.SignalR.Relay;

/// <summary>Tracks admitted clients and shares one shutdown deadline across drain callers.</summary>
internal sealed class RelayDrain : IRelayDrain, IHostedLifecycleService
{
    /// <summary>Maximum time to wait for admitted clients to leave.</summary>
    private readonly TimeSpan _timeout;

    /// <summary>Reads and validates the drain timeout once for this relay.</summary>
    /// <param name="configuration">Configuration supplied by the relay host.</param>
    public RelayDrain(IConfiguration configuration)
    {
        int drainSeconds = configuration.GetValue<int?>("Relay:DrainSeconds") ?? 5;
        if (drainSeconds is < 0 or > 120) throw new InvalidOperationException("Relay:DrainSeconds must be between 0 and 120.");
        _timeout = TimeSpan.FromSeconds(drainSeconds);
    }

    /// <summary>Serializes client admission, departure, and the transition to draining.</summary>
    private readonly AsyncLock _gate = new();

    /// <summary>Completes when all clients admitted before shutdown have left.</summary>
    private readonly TaskCompletionSource _empty = new(TaskCreationOptions.RunContinuationsAsynchronously);

    /// <summary>Number of outstanding client admission leases.</summary>
    private int _clients;

    /// <summary>Set when the relay stops admitting clients.</summary>
    private ValueAtomicBool _draining;
    public bool IsDraining => _draining.Read();

    public async ValueTask<IAsyncDisposable?> AdmitClientAsync()
    {
        using (await _gate.Lock())
        {
            if (IsDraining) return null;
            _clients++;
            return new RelayDrainLease(LeaveAsync);
        }
    }

    /// <summary>Releases one admission and signals an empty relay to drain callers.</summary>
    private async ValueTask LeaveAsync()
    {
        using (await _gate.Lock())
            if (--_clients == 0 && IsDraining) _empty.TrySetResult();
    }

    /// <summary>Shared wait for all clients to leave or for the timeout to elapse.</summary>
    private Task? _window;

    public async Task BeginAsync(CancellationToken cancellationToken)
    {
        Task window;
        using (await _gate.Lock())
        {
            if (_window is null)
            {
                _draining.VolatileWrite(true);
                if (_clients == 0) _empty.TrySetResult();
                _window = WaitForClients();
            }
            window = _window;
        }

        await window.WaitAsync(cancellationToken);
    }

    /// <summary>Waits for the last lease to be released, bounded by the configured drain timeout.</summary>
    private async Task WaitForClients()
    {
        try { await _empty.Task.WaitAsync(_timeout); }
        catch (TimeoutException) { }
    }

    public async Task StoppingAsync(CancellationToken token)
    {
        try { await BeginAsync(token); }
        catch (OperationCanceledException) when (token.IsCancellationRequested) { }
    }
    public Task StartingAsync(CancellationToken token) => Task.CompletedTask;
    public Task StartAsync(CancellationToken token) => Task.CompletedTask;
    public Task StartedAsync(CancellationToken token) => Task.CompletedTask;
    public Task StopAsync(CancellationToken token) => Task.CompletedTask;
    public Task StoppedAsync(CancellationToken token) => Task.CompletedTask;
}
