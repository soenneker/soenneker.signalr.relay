namespace Soenneker.Cloudflare.SignalR;

internal sealed class RelayDrain(TimeSpan timeout) : IRelayDrain, IHostedLifecycleService
{
    private readonly Lock _gate = new();
    private readonly TaskCompletionSource _empty = new(TaskCreationOptions.RunContinuationsAsynchronously);
    private int _clients;
    private bool _draining;
    public bool IsDraining => Volatile.Read(ref _draining);

    public IDisposable? AdmitClient()
    {
        lock (_gate)
        {
            if (_draining) return null;
            _clients++;
            return new RelayDrainLease(Leave);
        }
    }

    private void Leave()
    {
        lock (_gate)
            if (--_clients == 0 && _draining) _empty.TrySetResult();
    }

    private Task? _window;

    public Task BeginAsync(CancellationToken cancellationToken)
    {
        lock (_gate)
        {
            if (_window is not null) return _window.WaitAsync(cancellationToken);
            Volatile.Write(ref _draining, true);
            if (_clients == 0) _empty.TrySetResult();
            _window = WaitForClients();
            return _window.WaitAsync(cancellationToken);
        }
    }

    private async Task WaitForClients()
    {
        try { await _empty.Task.WaitAsync(timeout); }
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
