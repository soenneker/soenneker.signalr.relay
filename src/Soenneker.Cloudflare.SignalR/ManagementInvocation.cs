using System.Text.Json;
using System.Text.Json.Serialization;

namespace Soenneker.Cloudflare.SignalR;

[JsonUnmappedMemberHandling(JsonUnmappedMemberHandling.Disallow)]
internal sealed record ManagementInvocation(string Target, JsonElement[]? Arguments);
