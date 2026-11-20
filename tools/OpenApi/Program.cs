using System.Text.Json;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Routing;
using Soenneker.SignalR.Relay;

if (args.Length != 1) throw new ArgumentException("Supply an output path for openapi.json.");
await using var relay = RelayApplication.Build(["--Relay:Endpoint=https://relay.invalid", "--Relay:AccessKey=" + new string('x', 32)]);
var endpoints = ((IEndpointRouteBuilder)relay).DataSources.SelectMany(source => source.Endpoints).OfType<RouteEndpoint>();
var paths = new SortedDictionary<string, Dictionary<string, object>>(StringComparer.Ordinal);
foreach (var endpoint in endpoints)
{
    string path = endpoint.RoutePattern.RawText!;
    if (path.StartsWith("/client", StringComparison.Ordinal) || path.StartsWith("/server", StringComparison.Ordinal)) continue;
    var methods = endpoint.Metadata.GetRequiredMetadata<HttpMethodMetadata>().HttpMethods;
    foreach (string method in methods)
    {
        string operation = (method, path.Replace("/api/hubs/{hub}", "")) switch
        {
            ("GET", "/health") => "GetHealth",
            ("GET", "/api/health") => "GetServiceHealth",
            ("HEAD", "/api/health") => "CheckServiceHealth",
            ("GET", "/ready/{hub}") => "CheckHubReadiness",
            ("POST", "/:send") => "Broadcast",
            ("POST", "/connections/{id}/:send") => "SendToConnection",
            ("POST", "/connections/{id}/:invoke") => "InvokeClient",
            ("POST", "/groups/{group}/:send") => "SendToGroup",
            ("POST", "/users/{user}/:send") => "SendToUser",
            ("HEAD", "/groups/{group}") => "CheckGroup",
            ("HEAD", "/users/{user}") => "CheckUser",
            ("HEAD", "/connections/{id}") => "CheckConnection",
            ("DELETE", "/connections/{id}") => "CloseConnection",
            ("PUT", "/groups/{group}/connections/{id}") => "JoinGroup",
            ("DELETE", "/groups/{group}/connections/{id}") => "LeaveGroup",
            ("DELETE", "/connections/{id}/groups") => "LeaveAllGroups",
            _ => throw new InvalidOperationException($"Describe the new relay operation {method} {path}.")
        };
        var parameters = new List<object>();
        foreach (var p in endpoint.RoutePattern.Parameters)
            parameters.Add(new { name = p.Name, @in = "path", required = true, schema = new { type = "string" } });
        var responses = new SortedDictionary<string, object>();
        void Status(int status, string description) => responses[status.ToString()] = new { description };
        var spec = new Dictionary<string, object> { ["operationId"] = operation, ["parameters"] = parameters, ["responses"] = responses };
        bool authorized = endpoint.Metadata.GetMetadata<IAuthorizeData>() != null;
        if (authorized)
        {
            spec["security"] = new[] { new { bearer = Array.Empty<string>() } };
            Status(401, "Missing or invalid path-scoped bearer token.");
            Status(400, "Invalid request or unsupported application query parameter.");
        }
        Status(200, "Success.");
        if (operation == "CheckHubReadiness") Status(503, "Draining or no server tunnel is ready.");
        if (authorized && method is "HEAD" or "PUT" || operation == "CloseConnection")
            responses["404"] = new { description = "Resource missing.", headers = new Dictionary<string, object> { ["x-ms-error-code"] = new { schema = new { type = "string" } } } };
        if (method == "POST")
        {
            bool invoke = operation == "InvokeClient";
            parameters.Add(new { name = "Idempotency-Key", @in = "header", description = "Optional nonempty GUID. Reuse for the same request within the five-minute retention window.", schema = new { type = "string", format = "uuid" } });
            parameters.Add(new { name = "api-version", @in = "query", schema = new { type = "string" } });
            if (!invoke) parameters.Add(new { name = "excluded", @in = "query", description = "Connection IDs omitted from delivery.", style = "form", explode = true, schema = new { type = "array", items = new { type = "string" } } });
            spec["requestBody"] = new { required = true, content = new Dictionary<string, object> { ["application/json"] = new { schema = new Dictionary<string,string> { ["$ref"] = "#/components/schemas/ManagementInvocation" } } } };
            Status(409, "The idempotency key was used for a different request.");
            Status(413, "Request or retained reply exceeds its size limit.");
            Status(415, "A JSON request body is required.");
            Status(429, "The relay has reached its operation limit.");
            if (invoke || operation == "SendToConnection") Status(404, "The target connection was not found.");
            if (invoke)
            {
                responses["200"] = new { description = "SignalR JSON completion terminated by byte 0x1e.", content = new Dictionary<string,object> { ["application/octet-stream"] = new { schema = new { type = "string", format = "binary" } } }, headers = new Dictionary<string, object> { ["X-ASRS-Protocol"] = new { schema = new { type = "string", example = "json" } } } };
                Status(410, "Client disconnected.");
                Status(504, "Client completion timed out.");
            }
            else { responses.Remove("200"); Status(202, "Send accepted."); }
        }
        if (!paths.TryGetValue(path, out var verbs)) paths.Add(path, verbs = []);
        verbs.Add(method.ToLowerInvariant(), spec);
    }
}
var document = new {
    openapi = "3.0.3",
    info = new { title = "SignalR relay Relay Management", version = "1.0.0" },
    paths,
    components = new {
        securitySchemes = new { bearer = new { type = "http", scheme = "bearer", bearerFormat = "JWT", description = "HS256 or HS512 JWT with issuer azure-signalr and an audience matching the relay scheme, host and escaped request path. Omit the port and query." } },
        schemas = new { ManagementInvocation = new { type = "object", required = new[] { "target" }, additionalProperties = false, properties = new { target = new { type = "string", minLength = 1 }, arguments = new { type = "array", nullable = true, items = new Dictionary<string,object>(), description = "Ordered JSON arguments of any type." } } } }
    }
};
var output = Path.GetFullPath(args[0]);
Directory.CreateDirectory(Path.GetDirectoryName(output)!);
await File.WriteAllTextAsync(output, JsonSerializer.Serialize(document, new JsonSerializerOptions { WriteIndented = true }) + "\n");
