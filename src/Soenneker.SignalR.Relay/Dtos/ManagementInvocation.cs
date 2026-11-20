using System.Text.Json;
using System.Text.Json.Serialization;

namespace Soenneker.SignalR.Relay.Dtos;

/// <summary>Represents the JSON request body for a management send or invocation.</summary>
/// <param name="Target">Client method to invoke.</param>
/// <param name="Arguments">Ordered arguments to pass to the method. An omitted array represents an empty argument list.</param>
[JsonUnmappedMemberHandling(JsonUnmappedMemberHandling.Disallow)]
internal sealed record ManagementInvocation(string Target, JsonElement[]? Arguments);
