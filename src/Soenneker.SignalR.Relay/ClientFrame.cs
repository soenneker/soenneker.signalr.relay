using System.Buffers;
using System.Text.Json;
using Soenneker.Utils.Json;
using MessagePack;

namespace Soenneker.SignalR.Relay;

/// <summary>Reads bounded SignalR frames and translates client completions.</summary>
internal static class ClientFrame
{
    /// <summary>Maximum permitted frame body size in bytes.</summary>
    public const int MaximumLength = 1024 * 1024;

    /// <summary>Consumes one complete frame, including its length prefix or record separator.</summary>
    /// <param name="input">Unread transport bytes. Advanced only on success.</param>
    /// <param name="binary">Whether to read a length-prefixed MessagePack frame.</param>
    /// <param name="frame">Complete frame including its framing bytes.</param>
    /// <param name="body">Frame contents without framing bytes.</param>
    /// <returns>True if a complete frame was available.</returns>
    /// <exception cref="InvalidDataException">The frame exceeds the supported size.</exception>
    public static bool TryRead(ref ReadOnlySequence<byte> input, bool binary, out ReadOnlySequence<byte> frame, out ReadOnlySequence<byte> body)
    {
        frame = body = default;
        var reader = new SequenceReader<byte>(input);
        if (binary)
        {
            int length = 0;
            for (int shift = 0; ; shift += 7)
            {
                if (!reader.TryRead(out byte part)) return false;
                if (shift > 14) throw new InvalidDataException("Client message exceeds the frame limit.");
                length |= (part & 127) << shift;
                if (length > MaximumLength) throw new InvalidDataException("Client message exceeds the frame limit.");
                if ((part & 128) == 0) break;
            }
            if (reader.Remaining < length) return false;
            body = input.Slice(reader.Position, length);
            reader.Advance(length);
        }
        else if (!reader.TryReadTo(out body, (byte)0x1e))
        {
            if (input.Length > MaximumLength) throw new InvalidDataException("Client message exceeds the frame limit.");
            return false;
        }
        if (body.Length > MaximumLength) throw new InvalidDataException("Client message exceeds the frame limit.");
        frame = input.Slice(0, reader.Position);
        input = input.Slice(reader.Position);
        return true;
    }

    /// <summary>Encodes a MessagePack completion as a record-separated JSON completion.</summary>
    /// <param name="body">MessagePack completion body.</param>
    /// <param name="id">Validated invocation identifier.</param>
    /// <returns>The framed JSON reply.</returns>
    public static byte[] JsonCompletion(ReadOnlySequence<byte> body, string id)
    {
        using var reader = new MessagePackReader(body);
        reader.ReadArrayHeader();
        reader.ReadInt32();
        reader.Skip(); // Headers and ID were validated by CompletionId.
        reader.ReadString();
        var buffer = new ArrayBufferWriter<byte>();
        using (var writer = new Utf8JsonWriter(buffer))
        {
            writer.WriteStartObject();
            writer.WriteNumber("type", 3);
            writer.WriteString("invocationId", id);
            switch (reader.ReadInt32())
            {
                case 1:
                    writer.WriteString("error", reader.ReadString());
                    break;
                case 2:
                    break;
                case 3:
                    writer.WritePropertyName("result");
                    var result = JsonUtil.Deserialize(MessagePackSerializer.ConvertToJson(reader.ReadRaw()), RelayJsonContext.Default.JsonElement);
                    result.WriteTo(writer);
                    break;
                default:
                    throw new InvalidDataException("Invalid completion result.");
            }
            writer.WriteEndObject();
        }
        return [.. buffer.WrittenSpan, 0x1e];
    }

    /// <summary>Reads the invocation identifier from a client completion.</summary>
    /// <param name="body">Message body without framing bytes.</param>
    /// <param name="binary">Whether the body uses MessagePack.</param>
    /// <returns>The invocation identifier, or null for a message without a completion identifier.</returns>
    public static string? CompletionId(ReadOnlySequence<byte> body, bool binary)
    {
        if (binary)
        {
            using var reader = new MessagePackReader(body);
            if (reader.ReadArrayHeader() < 4 || reader.ReadInt32() != 3) return null;
            // Completion headers are a flat map of strings. Bound the work before reaching the invocation ID.
            int headers = reader.ReadMapHeader();
            if (headers > 128) throw new InvalidDataException("Too many completion headers.");
            for (int i = 0; i < headers; i++)
            {
                reader.ReadString();
                reader.ReadString();
            }
            return reader.ReadString();
        }

        var json = new Utf8JsonReader(body);
        if (!json.Read() || json.TokenType != JsonTokenType.StartObject) return null;
        string? id = null;
        bool completion = false;
        while (json.Read() && json.TokenType == JsonTokenType.PropertyName)
        {
            bool type = json.ValueTextEquals("type"u8);
            bool invocationId = json.ValueTextEquals("invocationId"u8);
            if (!json.Read()) return null;
            if (type)
            {
                if (json.TokenType != JsonTokenType.Number || !json.TryGetInt32(out var value) || value != 3) return null;
                completion = true;
            }
            if (invocationId && json.TokenType == JsonTokenType.String) id = json.GetString();
            json.Skip();
        }
        return completion ? id : null;
    }
}
