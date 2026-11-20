using System.Buffers;
using System.Text.Json;
using Soenneker.Utils.Json;
using MessagePack;

using Soenneker.SignalR.Relay.Abstract;
using Soenneker.SignalR.Relay.Dtos;

namespace Soenneker.SignalR.Relay;

/// <summary>Translates HTTP management requests into client hub messages.</summary>
internal static class ManagementSend
{
    /// <summary>Validates a management request and dispatches it to the requested hub audience.</summary>
    /// <param name="http">HTTP context for the current request.</param>
    /// <param name="directory">Hub and session directory for this relay.</param>
    /// <param name="hub">Requested hub name.</param>
    /// <returns>The HTTP result for the send or client invocation.</returns>
    internal static async Task<IResult> Handle(HttpContext http, IRelayDirectory directory, string hub)
    {
        if (!http.Request.HasJsonContentType()) return Results.StatusCode(StatusCodes.Status415UnsupportedMediaType);
        if (http.Request.Query.Keys.Any(k => k is not ("excluded" or "api-version"))) return Results.BadRequest();
        ManagementInvocation? message;
        try
        {
            message = await JsonSerializer.DeserializeAsync(http.Request.Body, RelayJsonContext.Default.ManagementInvocation, http.RequestAborted);
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
        var json = JsonUtil.SerializeToUtf8Bytes(new ClientInvocationFrame(1, invocationId, message.Target, args), RelayJsonContext.Default.ClientInvocationFrame);
        Array.Resize(ref json, json.Length + 1);
        json[^1] = 0x1e;
        var binary = new ArrayBufferWriter<byte>();
        var writer = new MessagePackWriter();
        try
        {
            writer.WriteArrayHeader(6);
            writer.WriteInt32(1);
            writer.WriteMapHeader(0);
            writer.WriteString(invocationId);
            writer.WriteString(message.Target);
            writer.WriteArrayHeader(args.Length);
            foreach (var arg in args) WriteArgument(ref writer, arg);
            writer.WriteArrayHeader(0);

            int length = checked((int)writer.BytesWritten);
            do
            {
                int next = length >> 7;
                binary.GetSpan(1)[0] = (byte)((length & 127) | (next > 0 ? 128 : 0));
                binary.Advance(1);
                length = next;
            } while (length > 0);
            writer.WriteTo(binary);
        }
        finally
        {
            writer.Dispose();
        }
        var routes = http.Request.RouteValues;
        var exclusions = http.Request.Query["excluded"].OfType<string>().ToHashSet(StringComparer.Ordinal);
        string? fingerprint = null;
        if (operation.HasValue)
        {
            using var hash = System.Security.Cryptography.IncrementalHash.CreateHash(System.Security.Cryptography.HashAlgorithmName.SHA256);
            hash.AppendData(JsonUtil.SerializeToUtf8Bytes(new ManagementAudience(http.Request.Path.Value, exclusions.Order(StringComparer.Ordinal).ToArray()), RelayJsonContext.Default.ManagementAudience));
            hash.AppendData(invoke ? JsonUtil.SerializeToUtf8Bytes(message, RelayJsonContext.Fingerprint.ManagementInvocation) : json);
            fingerprint = Convert.ToHexString(hash.GetHashAndReset());
        }
        if (invoke) return await target.InvokeManaged(http, (string)routes["id"]!, invocationId!, json, binary.WrittenMemory, operation, fingerprint);
        int status = target.SendManaged(routes["id"] as string, routes["group"] as string, routes["user"] as string,
            exclusions, json, binary.WrittenMemory, operation, fingerprint);
        return Results.StatusCode(status);
    }

    /// <summary>Writes a JSON argument recursively as a MessagePack value.</summary>
    /// <param name="writer">Destination for the encoded argument.</param>
    /// <param name="value">Value to encode.</param>
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
                    writer.WriteString(item.Name);
                    WriteArgument(ref writer, item.Value);
                }
                break;
            case JsonValueKind.Number:
                if (value.TryGetInt64(out var signed)) writer.WriteInt64(signed);
                else if (value.TryGetUInt64(out var unsigned)) writer.WriteUInt64(unsigned);
                else writer.WriteDouble(value.GetDouble());
                break;
            case JsonValueKind.String: writer.WriteString(value.GetString()); break;
            case JsonValueKind.Null: writer.WriteNil(); break;
            default: writer.WriteBoolean(value.GetBoolean()); break;
        }
    }
}
