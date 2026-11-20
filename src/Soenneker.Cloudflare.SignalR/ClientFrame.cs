using System.Buffers;
using System.Text.Json;
using MessagePack;

namespace Soenneker.Cloudflare.SignalR;

internal static class ClientFrame
{
    public const int MaximumLength = 1024 * 1024;

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

    public static byte[] JsonCompletion(ReadOnlySequence<byte> body, string id)
    {
        var reader = new MessagePackReader(body);
        reader.ReadArrayHeader();
        reader.ReadInt32();
        reader.Skip(); // Headers and ID were validated by CompletionId.
        reader.ReadString();
        byte[] json = reader.ReadInt32() switch
        {
            1 => JsonSerializer.SerializeToUtf8Bytes(new { type = 3, invocationId = id, error = reader.ReadString() }),
            2 => JsonSerializer.SerializeToUtf8Bytes(new { type = 3, invocationId = id }),
            3 => JsonSerializer.SerializeToUtf8Bytes(new { type = 3, invocationId = id,
                result = JsonSerializer.Deserialize<JsonElement>(MessagePackSerializer.ConvertToJson(reader.ReadRaw().ToArray(), MessagePackSerializerOptions.Standard.WithSecurity(MessagePackSecurity.UntrustedData))) }),
            _ => throw new InvalidDataException("Invalid completion result.")
        };
        return [.. json, 0x1e];
    }

    public static string? CompletionId(ReadOnlySequence<byte> body, bool binary)
    {
        if (binary)
        {
            var reader = new MessagePackReader(body);
            if (reader.ReadArrayHeader() < 4 || reader.ReadInt32() != 3) return null;
            // Completion headers are a flat map of strings. Bound the work before reaching the invocation ID.
            int headers = reader.ReadMapHeader();
            if (headers > 128) throw new InvalidDataException("Too many completion headers.");
            for (int i = 0; i < headers; i++)
            {
                reader.ReadStringSequence();
                reader.ReadStringSequence();
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
