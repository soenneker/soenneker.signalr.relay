using System.Buffers;
using System.Text.Json;
using MessagePack;

namespace Soenneker.Cloudflare.SignalR;

// One instance per negotiated, live WebSocket session. Only the output pump assigns outgoing sequence numbers.
internal sealed class RelayReplay(bool binary) : IDisposable
{
    private static long _retainedBytes;
    private readonly Lock _gate = new();
    private readonly Queue<(long Sequence, ReadOnlyMemory<byte> Frame)> _pending = new();
    private int _bytes;
    private long _sent;
    private long _received;
    private long _next = 1;
    private bool _handshake = true;

    public void Remember(ReadOnlyMemory<byte> data)
    {
        lock (_gate)
        {
            var input = new ReadOnlySequence<byte>(data);
            if (_handshake)
            {
                if (!ClientFrame.TryRead(ref input, false, out _, out _)) throw new InvalidDataException("Incomplete handshake response.");
                _handshake = false;
            }
            while (!input.IsEmpty)
            {
                if (!ClientFrame.TryRead(ref input, binary, out var frame, out var body)) throw new InvalidDataException("Incomplete outgoing hub frame.");
                if (ReadHeader(body).Type is < 1 or > 5) continue;
                if (_pending.Count >= 1024 || frame.Length + _bytes > 1024 * 1024) throw new InvalidDataException("Reconnect buffer limit reached.");
                if (Interlocked.Add(ref _retainedBytes, frame.Length) > 64L * 1024 * 1024)
                {
                    Interlocked.Add(ref _retainedBytes, -frame.Length);
                    throw new InvalidDataException("Relay reconnect buffer limit reached.");
                }
                var bytes = frame.ToArray();
                _bytes += bytes.Length;
                _pending.Enqueue((++_sent, bytes));
            }
        }
    }

    public ReadOnlyMemory<byte> Replay()
    {
        lock (_gate)
        {
            if (_handshake) throw new InvalidDataException("Connection interrupted before handshake.");
            var output = new ArrayBufferWriter<byte>();
            output.Write(Control(9, _pending.TryPeek(out var first) ? first.Sequence : _sent + 1));
            if (_received > 0) output.Write(Control(8, _received));
            foreach (var item in _pending) output.Write(item.Frame.Span);
            return output.WrittenMemory;
        }
    }

    public bool Accept(ReadOnlySequence<byte> body, RelayPeer peer)
    {
        var (type, sequence) = ReadHeader(body);
        lock (_gate)
        {
            if (type == 8)
            {
                if (sequence < 0 || sequence > _sent) throw new InvalidDataException("Invalid acknowledgement.");
                while (_pending.TryPeek(out var item) && item.Sequence <= sequence)
                {
                    int released = _pending.Dequeue().Frame.Length;
                    _bytes -= released;
                    Interlocked.Add(ref _retainedBytes, -released);
                }
                return false;
            }
            if (type == 9)
            {
                if (sequence < 1 || sequence > _received + 1) throw new InvalidDataException("Invalid reconnect sequence.");
                _next = sequence;
                return false;
            }
            if (type is < 1 or > 5) return true;
            var position = _next++;
            if (position <= _received) return false;
            _received = position;
            peer.Send(Control(8, position));
            return true;
        }
    }

    public void Dispose()
    {
        lock (_gate)
        {
            Interlocked.Add(ref _retainedBytes, -_bytes);
            _bytes = 0;
            _pending.Clear();
        }
    }

    private (int Type, long Sequence) ReadHeader(ReadOnlySequence<byte> body)
    {
        if (binary)
        {
            var reader = new MessagePackReader(body);
            if (reader.ReadArrayHeader() < 1) throw new InvalidDataException("Empty hub message.");
            var type = reader.ReadInt32();
            return (type, type is 8 or 9 ? reader.ReadInt64() : 0);
        }
        using var document = JsonDocument.Parse(body);
        var root = document.RootElement;
        var kind = root.GetProperty("type").GetInt32();
        return (kind, kind is 8 or 9 ? root.GetProperty("sequenceId").GetInt64() : 0);
    }

    private byte[] Control(int type, long sequence)
    {
        if (!binary)
        {
            var json = JsonSerializer.SerializeToUtf8Bytes(new { type, sequenceId = sequence });
            return [.. json, 0x1e];
        }
        var buffer = new ArrayBufferWriter<byte>();
        var writer = new MessagePackWriter(buffer);
        writer.WriteArrayHeader(2);
        writer.Write(type);
        writer.Write(sequence);
        writer.Flush();
        return [(byte)buffer.WrittenCount, .. buffer.WrittenSpan];
    }
}
