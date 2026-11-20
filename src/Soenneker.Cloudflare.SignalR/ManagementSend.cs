using System.Buffers;
using System.Text.Json;
using MessagePack;

namespace Soenneker.Cloudflare.SignalR;

internal static class ManagementSend
{
    private static readonly JsonSerializerOptions Json = new(JsonSerializerDefaults.Web);

    internal static async Task<IResult> Handle(HttpContext http, IRelayDirectory directory, string hub)
    {
        if (!http.Request.HasJsonContentType()) return Results.StatusCode(StatusCodes.Status415UnsupportedMediaType);
        if (http.Request.Query.Keys.Any(k => k is not ("excluded" or "api-version"))) return Results.BadRequest();
        ManagementInvocation? message;
        try
        {
            message = await JsonSerializer.DeserializeAsync<ManagementInvocation>(http.Request.Body, Json, http.RequestAborted);
        }
        catch (JsonException) { return Results.BadRequest(); }
        if (string.IsNullOrEmpty(message?.Target)) return Results.BadRequest();

        Guid? operation = null;
        if (http.Request.Headers.TryGetValue("Idempotency-Key", out var keys))
        {
            if (keys.Count != 1 || !Guid.TryParseExact(keys[0], "D", out var value) || value == Guid.Empty) return Results.BadRequest();
            operation = value;
        }
        var target = operation.HasValue ? directory.Get(hub) : directory.Find(hub);
        bool invoke = http.Request.Path.Value!.EndsWith("/:invoke", StringComparison.OrdinalIgnoreCase);
        if (target is null) return invoke ? Results.NotFound() : Results.Accepted();
        string? invocationId = invoke ? $"cf:{Guid.NewGuid()}" : null;
        var args = message.Arguments ?? [];
        var json = invoke ? JsonSerializer.SerializeToUtf8Bytes(new { type = 1, invocationId, target = message.Target, arguments = args }) : JsonSerializer.SerializeToUtf8Bytes(new { type = 1, target = message.Target, arguments = args });
        Array.Resize(ref json, json.Length + 1);
        json[^1] = 0x1e;
        var packed = new ArrayBufferWriter<byte>();
        var writer = new MessagePackWriter(packed);
        writer.WriteArrayHeader(6);
        writer.Write(1);
        writer.WriteMapHeader(0);
        writer.Write(invocationId);
        writer.Write(message.Target);
        writer.WriteArrayHeader(args.Length);
        foreach (var arg in args) WriteArgument(ref writer, arg);
        writer.WriteArrayHeader(0);
        writer.Flush();

        int length = packed.WrittenCount;
        var binary = new ArrayBufferWriter<byte>();
        do
        {
            int next = length >> 7;
            binary.GetSpan(1)[0] = (byte)((length & 127) | (next > 0 ? 128 : 0));
            binary.Advance(1);
            length = next;
        } while (length > 0);
        binary.Write(packed.WrittenSpan);
        var routes = http.Request.RouteValues;
        var exclusions = http.Request.Query["excluded"].OfType<string>().ToHashSet(StringComparer.Ordinal);
        string? fingerprint = null;
        if (operation.HasValue)
        {
            using var hash = System.Security.Cryptography.IncrementalHash.CreateHash(System.Security.Cryptography.HashAlgorithmName.SHA256);
            hash.AppendData(JsonSerializer.SerializeToUtf8Bytes(new { path = http.Request.Path.Value, excluded = exclusions.Order(StringComparer.Ordinal) }));
            hash.AppendData(invoke ? JsonSerializer.SerializeToUtf8Bytes(message) : json);
            fingerprint = Convert.ToHexString(hash.GetHashAndReset());
        }
        if (invoke) return await target.InvokeManaged(http, (string)routes["id"]!, invocationId!, json, binary.WrittenMemory, operation, fingerprint);
        int status = target.SendManaged(routes["id"] as string, routes["group"] as string, routes["user"] as string,
            exclusions, json, binary.WrittenMemory, operation, fingerprint);
        return Results.StatusCode(status);
    }

    private static void WriteArgument(ref MessagePackWriter writer, JsonElement value)
    {
        switch (value.ValueKind)
        {
            case JsonValueKind.Array:
                writer.WriteArrayHeader(value.GetArrayLength());
                foreach (var item in value.EnumerateArray()) WriteArgument(ref writer, item);
                break;
            case JsonValueKind.Object:
                writer.WriteMapHeader(value.GetPropertyCount());
                foreach (var item in value.EnumerateObject())
                {
                    writer.Write(item.Name);
                    WriteArgument(ref writer, item.Value);
                }
                break;
            case JsonValueKind.Number:
                if (value.TryGetInt64(out var signed)) writer.Write(signed);
                else if (value.TryGetUInt64(out var unsigned)) writer.Write(unsigned);
                else writer.Write(value.GetDouble());
                break;
            case JsonValueKind.String: writer.Write(value.GetString()); break;
            case JsonValueKind.Null: writer.WriteNil(); break;
            default: writer.Write(value.GetBoolean()); break;
        }
    }
}
