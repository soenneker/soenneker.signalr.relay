using System.Text.Json;
using System.Text.Json.Serialization;

namespace Soenneker.SignalR.Relay.Dtos;

/// <summary>A JSON invocation sent to a SignalR client.</summary>
/// <param name="Type">Hub message type.</param>
/// <param name="InvocationId">Correlation identifier when a result is requested.</param>
/// <param name="Target">Client method name.</param>
/// <param name="Arguments">Arguments in invocation order.</param>
internal sealed record ClientInvocationFrame(int Type, [property: JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)] string? InvocationId, string Target, JsonElement[] Arguments);
