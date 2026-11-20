namespace Soenneker.SignalR.Relay.Abstract;

/// <summary>Coordinates client admission and graceful relay shutdown.</summary>
internal interface IRelayDrain
{
    /// <summary>Gets whether draining has begun and new clients are being refused.</summary>
    bool IsDraining { get; }

    /// <summary>Attempts to admit a client before draining begins.</summary>
    /// <returns>A lease to dispose asynchronously when the client leaves, or <see langword="null"/> if draining has begun.</returns>
    ValueTask<IAsyncDisposable?> AdmitClientAsync();

    /// <summary>Closes admission and waits for admitted clients to leave or the drain timeout to elapse.</summary>
    /// <param name="cancellationToken">Cancels this caller’s wait while draining continues.</param>
    /// <returns>A task that represents this caller’s wait for the drain to finish.</returns>
    Task BeginAsync(CancellationToken cancellationToken);
}
