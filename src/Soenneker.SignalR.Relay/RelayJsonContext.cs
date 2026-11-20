using System.Text.Json;
using System.Text.Json.Serialization;
using Soenneker.SignalR.Relay.Dtos;

namespace Soenneker.SignalR.Relay;

/// <summary>Supplies generated JSON contracts for management requests and SignalR frames.</summary>
[JsonSourceGenerationOptions(JsonSerializerDefaults.Web)]
[JsonSerializable(typeof(ManagementInvocation))]
[JsonSerializable(typeof(ClientInvocationFrame))]
[JsonSerializable(typeof(ReplayControlFrame))]
[JsonSerializable(typeof(ManagementAudience))]
[JsonSerializable(typeof(JsonElement))]
internal sealed partial class RelayJsonContext : JsonSerializerContext
{
    /// <summary>Preserves the original property casing used to hash management invocation bodies.</summary>
    internal static readonly RelayJsonContext Fingerprint = new(new JsonSerializerOptions(JsonSerializerDefaults.General));
}
