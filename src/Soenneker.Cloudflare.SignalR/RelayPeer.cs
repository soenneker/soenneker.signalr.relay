using System.Threading.Channels;
using System.IO.Pipelines;
using Microsoft.AspNetCore.Connections;
using Microsoft.Azure.SignalR.Protocol;

namespace Soenneker.Cloudflare.SignalR;

internal sealed class RelayPeer(ConnectionContext connection, IRelayOutputBudget budget, string? serverId = null)
{
    private const int MaxPendingBytes = 4 * 1024 * 1024;
    private readonly Channel<ReadOnlyMemory<byte>> _output = Channel.CreateBounded<ReadOnlyMemory<byte>>(
        new BoundedChannelOptions(64) { SingleReader = false });
    private static readonly ServiceProtocol Protocol = new();
    private long _pendingBytes;
    private int _closed;
    private int _pendingMessages;

    private PipeWriter _writer = null!;
    private RelayReplay? _replay;
    private PipeWriter? _replacement;
    private static readonly ReadOnlyMemory<byte> ResumeMarker = new byte[0];

    public void EnableReplay(RelayReplay replay) => _replay = replay;

    public Task Resume(PipeWriter replacement)
    {
        if (Interlocked.CompareExchange(ref _replacement, replacement, null) != null ||
            !TryEnqueue(ResumeMarker, reserveControlCapacity: false))
            Abort();
        else
            Volatile.Read(ref _writer).CancelPendingFlush();
        return Task.CompletedTask;
    }

    public bool AllowsReconnect { get; set; }
    public string? ServerId => serverId;
    public string Id => connection.ConnectionId;

    internal long PendingBytes => _pendingBytes;
    internal bool HasQueuedData => _output.Reader.TryPeek(out _);

    public void Abort()
    {
        lock (budget.Gate) CloseQueue();
        AbortTransport();
    }

    private int _aborted;
    private void AbortTransport()
    {
        if (Interlocked.Exchange(ref _aborted, 1) == 0) connection.Abort();
    }

    // Caller holds the queue budget lock. The item being flushed stays charged until its write ends.
    internal void CloseQueue()
    {
        if (_closed != 0) return;
        Volatile.Write(ref _closed, 1);
        _output.Writer.TryComplete();
        while (_output.Reader.TryRead(out var data)) Release(data.Length);
    }

    private void Release(int bytes)
    {
        _pendingMessages--;
        _pendingBytes -= bytes;
        budget.Release(this, bytes);
    }

    public void Send(ServiceMessage message) => Send(Protocol.GetMessageBytes(message));

    public void Send(ReadOnlyMemory<byte> data)
    {
        if (!TryEnqueue(data, reserveControlCapacity: false))
            Abort();
    }

    // Inbound traffic from one client must leave room on the shared tunnel for close and group messages.
    public bool TryForward(ConnectionDataMessage message) =>
        TryEnqueue(Protocol.GetMessageBytes(message), reserveControlCapacity: true);

    private bool TryEnqueue(ReadOnlyMemory<byte> data, bool reserveControlCapacity)
    {
        List<RelayPeer>? evicted = null;
        bool accepted = false;
        lock (budget.Gate)
        {
            if (_closed != 0) return false;
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

    public async Task Pump(CancellationToken token)
    {
        _writer = connection.Transport.Output;
        try
        {
            await foreach (var data in _output.Reader.ReadAllAsync(token))
            {
                try
                {
                    if (Volatile.Read(ref _closed) != 0)
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
