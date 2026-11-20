using Soenneker.Atomics.ValueBools;
using System.Threading.Channels;
using System.IO.Pipelines;
using Microsoft.AspNetCore.Connections;
using Microsoft.Azure.SignalR.Protocol;

using Soenneker.SignalR.Relay.Abstract;

namespace Soenneker.SignalR.Relay;

/// <summary>Queues outgoing frames for a client connection or a server tunnel.</summary>
/// <param name="connection">Underlying transport connection.</param>
/// <param name="budget">Byte budget shared by peer output queues.</param>
/// <param name="serverId">Application server identifier, or null for a client transport.</param>
internal sealed class RelayPeer(ConnectionContext connection, IRelayOutputBudget budget, string? serverId = null)
{
    /// <summary>Maximum private queue budget for one peer, in bytes.</summary>
    private const int MaxPendingBytes = 4 * 1024 * 1024;

    /// <summary>Bounded channel drained by the transport output pump.</summary>
    private readonly Channel<ReadOnlyMemory<byte>> _output = Channel.CreateBounded<ReadOnlyMemory<byte>>(
        new BoundedChannelOptions(64) { SingleReader = false });

    /// <summary>Encoder for server tunnel messages.</summary>
    private static readonly ServiceProtocol Protocol = new();

    /// <summary>Bytes reserved for queued messages and the write in progress.</summary>
    private long _pendingBytes;

    /// <summary>Set once queue admission has closed.</summary>
    private ValueAtomicBool _closed;

    /// <summary>Number of queued messages including the write in progress.</summary>
    private int _pendingMessages;

    /// <summary>Transport writer currently owned by the pump.</summary>
    private PipeWriter _writer = null!;

    /// <summary>Optional replay buffer for a reconnectable client.</summary>
    private RelayReplay? _replay;

    /// <summary>Writer waiting for the pump to switch to a resumed transport.</summary>
    private PipeWriter? _replacement;

    /// <summary>Distinct queue item that orders transport replacement relative to data writes.</summary>
    private static readonly ReadOnlyMemory<byte> ResumeMarker = new byte[0];

    /// <summary>Enables retention of outbound frames for stateful reconnect.</summary>
    /// <param name="replay">Session buffer used to retain unacknowledged frames.</param>
    public void EnableReplay(RelayReplay replay) => _replay = replay;

    /// <summary>Queues a writer change and wakes the pump, aborting on a conflicting resume.</summary>
    /// <param name="replacement">Output writer for the resumed transport.</param>
    /// <returns>A completed task once the switch has been requested.</returns>
    public Task Resume(PipeWriter replacement)
    {
        if (Interlocked.CompareExchange(ref _replacement, replacement, null) != null ||
            !TryEnqueue(ResumeMarker, reserveControlCapacity: false))
            Abort();
        else
            Volatile.Read(ref _writer).CancelPendingFlush();
        return Task.CompletedTask;
    }

    /// <summary>Gets or sets whether the server tunnel advertises stateful reconnect.</summary>
    public bool AllowsReconnect { get; set; }

    /// <summary>Gets the server identifier for a tunnel, or null for a client peer.</summary>
    public string? ServerId => serverId;

    /// <summary>Gets the transport connection identifier.</summary>
    public string Id => connection.ConnectionId;

    /// <summary>Gets the number of outstanding bytes charged to this peer.</summary>
    internal long PendingBytes => _pendingBytes;

    /// <summary>Gets whether an enqueued item remains available to evict.</summary>
    internal bool HasQueuedData => _output.Reader.TryPeek(out _);

    /// <summary>Closes the output queue and aborts the underlying connection.</summary>
    public void Abort()
    {
        lock (budget.Gate) CloseQueue();
        AbortTransport();
    }

    /// <summary>Set when the transport has already been aborted.</summary>
    private ValueAtomicBool _aborted;

    /// <summary>Aborts the underlying connection at most once.</summary>
    private void AbortTransport()
    {
        if (_aborted.TrySetTrue()) connection.Abort();
    }

    // Caller holds the queue budget lock. The item being flushed stays charged until its write ends.
    /// <summary>Closes the queue under the shared budget lock and releases buffered items.</summary>
    internal void CloseQueue()
    {
        if (_closed.Read()) return;
        _closed.VolatileWrite(true);
        _output.Writer.TryComplete();
        while (_output.Reader.TryRead(out var data)) Release(data.Length);
    }

    /// <summary>Releases one message slot and its reserved bytes under the budget lock.</summary>
    /// <param name="bytes">Number of bytes to release.</param>
    private void Release(int bytes)
    {
        _pendingMessages--;
        _pendingBytes -= bytes;
        budget.Release(this, bytes);
    }

    /// <summary>Encodes and queues a service protocol message, aborting the peer on overflow.</summary>
    /// <param name="message">Incoming service protocol message.</param>
    public void Send(ServiceMessage message) => Send(Protocol.GetMessageBytes(message));

    /// <summary>Queues encoded bytes for transport and aborts the peer on overflow.</summary>
    /// <param name="data">Encoded data for the peer transport.</param>
    public void Send(ReadOnlyMemory<byte> data)
    {
        if (!TryEnqueue(data, reserveControlCapacity: false))
            Abort();
    }

    // Inbound traffic from one client must leave room on the shared tunnel for close and group messages.
    /// <summary>Attempts to forward client data while reserving tunnel capacity for control messages.</summary>
    /// <param name="message">Incoming service protocol message.</param>
    /// <returns>True if the message was queued.</returns>
    public bool TryForward(ConnectionDataMessage message) =>
        TryEnqueue(Protocol.GetMessageBytes(message), reserveControlCapacity: true);

    /// <summary>Reserves queue capacity and aborts any evicted clients after releasing the budget lock.</summary>
    /// <param name="data">Encoded data for the peer transport.</param>
    /// <param name="reserveControlCapacity">Whether to leave room for server tunnel control messages.</param>
    /// <returns>True if the item entered the output channel.</returns>
    private bool TryEnqueue(ReadOnlyMemory<byte> data, bool reserveControlCapacity)
    {
        List<RelayPeer>? evicted = null;
        bool accepted = false;
        lock (budget.Gate)
        {
            if (_closed.Read()) return false;
            var byteLimit = reserveControlCapacity ? 3 * MaxPendingBytes / 4 : MaxPendingBytes;
            var messageLimit = reserveControlCapacity ? 48 : 64;
            if (_pendingMessages < messageLimit && _pendingBytes + data.Length <= byteLimit &&
                budget.TryReserve(this, data.Length, ServerId is not null && !reserveControlCapacity, out evicted))
            {
                _pendingBytes += data.Length;
                _pendingMessages++;
                accepted = _output.Writer.TryWrite(data);
                if (!accepted) Release(data.Length);
            }
        }
        if (evicted is not null)
            foreach (var victim in evicted) victim.AbortTransport();
        if (!accepted) RelayMetrics.OutputOverflows.Add(1);
        return accepted;
    }

    /// <summary>Writes queued frames in order and switches writers when a reconnect marker arrives.</summary>
    /// <param name="token">Cancels the transport loop.</param>
    public async Task Pump(CancellationToken token)
    {
        _writer = connection.Transport.Output;
        try
        {
            await foreach (var data in _output.Reader.ReadAllAsync(token))
            {
                try
                {
                    if (_closed.Read())
                        break;

                    FlushResult result;
                    if (data.Equals(ResumeMarker))
                    {
                        var writer = Interlocked.Exchange(ref _replacement, null)!;
                        Volatile.Write(ref _writer, writer);
                        if (_replay is null) break;
                        result = await writer.WriteAsync(_replay.Replay(), token);
                    }
                    else
                    {
                        _replay?.Remember(data);
                        result = await _writer.WriteAsync(data, token);
                    }
                    if (_replay is null && (result.IsCompleted || result.IsCanceled))
                        break;
                }
                finally { lock (budget.Gate) Release(data.Length); }
            }
        }
        finally { Abort(); }
    }
}
