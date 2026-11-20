namespace Soenneker.SignalR.Relay.Abstract;

/// <summary>Accounts for queued output bytes across relay peers.</summary>
internal interface IRelayOutputBudget
{
    /// <summary>Gets the lock callers must hold while updating queues and reserving or releasing bytes.</summary>
    object Gate { get; }

    /// <summary>Attempts to reserve queue capacity, evicting queued clients if necessary.</summary>
    /// <param name="peer">The peer whose output will be queued.</param>
    /// <param name="bytes">The number of bytes to reserve.</param>
    /// <param name="serverControl">Whether the reservation may use capacity reserved for server control traffic.</param>
    /// <param name="evicted">Peers whose queues were closed, or <see langword="null"/> if none were evicted. Abort their transports after releasing the lock, even if the reservation failed.</param>
    /// <returns>True if the bytes were reserved.</returns>
    bool TryReserve(RelayPeer peer, int bytes, bool serverControl, out List<RelayPeer>? evicted);

    /// <summary>Returns bytes previously reserved for a peer to the shared budget.</summary>
    /// <param name="peer">The peer whose queued byte count has been updated.</param>
    /// <param name="bytes">The number of bytes to release.</param>
    void Release(RelayPeer peer, int bytes);
}
