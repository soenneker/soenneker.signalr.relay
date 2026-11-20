namespace Soenneker.SignalR.Relay;

/// <summary>Releases one client admission when its connection handler exits.</summary>
/// <param name="release">Callback that records the client's departure.</param>
internal sealed class RelayDrainLease(Func<ValueTask> release) : IAsyncDisposable
{
    /// <summary>Departure callback cleared on first disposal.</summary>
    private Func<ValueTask>? _release = release;
    public ValueTask DisposeAsync() => Interlocked.Exchange(ref _release, null)?.Invoke() ?? ValueTask.CompletedTask;
}
