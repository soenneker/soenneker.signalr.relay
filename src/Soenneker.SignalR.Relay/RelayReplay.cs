using Soenneker.Atomics.Longs;
using System.Buffers;
using System.Runtime.InteropServices;
using System.Text.Json;
using Soenneker.Utils.Json;
using Soenneker.SignalR.Relay.Dtos;
using MessagePack;

namespace Soenneker.SignalR.Relay;

// One instance per negotiated, live WebSocket session. Only the output pump assigns outgoing sequence numbers.
/// <summary>Tracks acknowledgements and unacknowledged frames for one stateful WebSocket session.</summary>
/// <param name="binary">Whether hub messages use MessagePack framing.</param>
internal sealed class RelayReplay(bool binary) : IDisposable
{
    /// <summary>Total replay bytes retained by all sessions.</summary>
    private static readonly AtomicLong _retainedBytes = new();

    /// <summary>Protects sequence numbers and retained frames for this session.</summary>
    private readonly Lock _gate = new();

    /// <summary>Unacknowledged outgoing frames in sequence order.</summary>
    private readonly Queue<(long Sequence, ReadOnlyMemory<byte> Frame)> _pending = new();

    /// <summary>Bytes retained for replay by this session.</summary>
    private int _bytes;

    /// <summary>Last sequence number assigned by the output pump.</summary>
    private long _sent;

    /// <summary>Highest incoming sequence accepted for forwarding.</summary>
    private long _received;

    /// <summary>Sequence number expected for the next incoming data frame.</summary>
    private long _next = 1;

    /// <summary>Indicates that the initial handshake response still needs to be consumed.</summary>
    private bool _handshake = true;

    /// <summary>Retains outgoing data frames after the handshake, subject to per-session and relay limits.</summary>
    /// <param name="data">Queue-owned bytes that remain immutable after admission.</param>
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
                if (_retainedBytes.Add(frame.Length) > 64L * 1024 * 1024)
                {
                    _retainedBytes.Add(-frame.Length);
                    throw new InvalidDataException("Relay reconnect buffer limit reached.");
                }
                // Retain an exact-size queue buffer directly. Copy slices so a small frame
                // cannot keep an entire transport batch alive outside the replay byte limit.
                ReadOnlyMemory<byte> bytes = frame.Length == data.Length &&
                    MemoryMarshal.TryGetArray(data, out var segment) && segment.Array?.Length == data.Length
                    ? data : frame.ToArray();
                _bytes += bytes.Length;
                _pending.Enqueue((++_sent, bytes));
            }
        }
    }

    /// <summary>Builds the sequence marker, optional acknowledgement, and outstanding frames for a resumed transport.</summary>
    /// <returns>Framed bytes to write first on the resumed transport.</returns>
    public ReadOnlyMemory<byte> Replay()
    {
        lock (_gate)
        {
            if (_handshake) throw new InvalidDataException("Connection interrupted before handshake.");
            var marker = Control(9, _pending.TryPeek(out var first) ? first.Sequence : _sent + 1);
            var acknowledgement = _received > 0 ? Control(8, _received) : [];
            if (_bytes == 0 && acknowledgement.Length == 0) return marker;
            var output = new byte[marker.Length + acknowledgement.Length + _bytes];
            marker.CopyTo(output, 0);
            acknowledgement.CopyTo(output, marker.Length);
            int offset = marker.Length + acknowledgement.Length;
            foreach (var item in _pending)
            {
                item.Frame.Span.CopyTo(output.AsSpan(offset));
                offset += item.Frame.Length;
            }
            return output;
        }
    }

    /// <summary>Processes reconnect control frames and acknowledges newly received data.</summary>
    /// <param name="body">Hub message body without framing bytes.</param>
    /// <param name="peer">Transport peer for this connection.</param>
    /// <returns>True when the frame should be forwarded to the server.</returns>
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
                    _retainedBytes.Add(-released);
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
            _retainedBytes.Add(-_bytes);
            _bytes = 0;
            _pending.Clear();
        }
    }

    /// <summary>Reads the hub message type and the sequence number of a reconnect control frame.</summary>
    /// <param name="body">Hub message body without framing bytes.</param>
    /// <returns>The message type and its sequence number, or zero for a message without a sequence.</returns>
    private (int Type, long Sequence) ReadHeader(ReadOnlySequence<byte> body)
    {
        if (binary)
        {
            using var reader = new MessagePackReader(body);
            if (reader.ReadArrayHeader() < 1) throw new InvalidDataException("Empty hub message.");
            var type = reader.ReadInt32();
            return (type, type is 8 or 9 ? reader.ReadInt64() : 0);
        }
        var json = new Utf8JsonReader(body);
        if (!json.Read() || json.TokenType != JsonTokenType.StartObject)
            throw new InvalidDataException("Expected a JSON hub message.");
        int? kind = null;
        long? sequence = null;
        while (json.Read() && json.TokenType == JsonTokenType.PropertyName)
        {
            bool isType = json.ValueTextEquals("type"u8);
            bool isSequence = json.ValueTextEquals("sequenceId"u8);
            if (!json.Read()) throw new InvalidDataException("Incomplete hub message.");
            if (isType)
                kind = json.TokenType == JsonTokenType.Number && json.TryGetInt32(out var value) ? value : null;
            else if (isSequence)
                sequence = json.TokenType == JsonTokenType.Number && json.TryGetInt64(out var number) ? number : null;
            json.Skip();
        }
        if (json.TokenType != JsonTokenType.EndObject || json.Read() || kind is null)
            throw new InvalidDataException("Invalid JSON hub message.");
        return (kind.Value, kind is 8 or 9 ? sequence ?? throw new InvalidDataException("Invalid reconnect sequence.") : 0);
    }

    /// <summary>Encodes an acknowledgement or sequence marker in the negotiated hub protocol.</summary>
    /// <param name="type">Hub protocol control message type.</param>
    /// <param name="sequence">Sequence number carried by the control message.</param>
    /// <returns>The complete control frame, ready to write.</returns>
    private byte[] Control(int type, long sequence)
    {
        if (!binary)
        {
            var json = JsonUtil.SerializeToUtf8Bytes(new ReplayControlFrame(type, sequence), RelayJsonContext.Default.ReplayControlFrame);
            return [.. json, 0x1e];
        }
        using var writer = new MessagePackWriter();
        writer.WriteArrayHeader(2);
        writer.WriteInt32(type);
        writer.WriteInt64(sequence);
        var frame = new byte[1 + writer.BytesWritten];
        frame[0] = (byte)writer.BytesWritten;
        writer.WriteTo(frame.AsSpan(1));
        return frame;
    }
}
