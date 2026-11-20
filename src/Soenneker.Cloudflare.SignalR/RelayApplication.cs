using Microsoft.AspNetCore.Authentication.JwtBearer;
using Microsoft.AspNetCore.Http.Connections;
using Microsoft.IdentityModel.Tokens;
using System.Text;

namespace Soenneker.Cloudflare.SignalR;

/// <summary>Builds the HTTP and WebSocket host for the Cloudflare SignalR relay.</summary>
public static class RelayApplication
{
    /// <summary>Creates a host configured from command-line arguments and environment variables.</summary>
    public static WebApplication Build(string[] args)
    {
        var builder = WebApplication.CreateBuilder(args);
        builder.Logging.AddFilter("Microsoft.AspNetCore.Hosting.Diagnostics", LogLevel.Warning); // Request URLs can carry browser bearer tokens.
        var endpoint = new Uri(builder.Configuration["Relay:Endpoint"] ?? throw new InvalidOperationException("Relay:Endpoint is required."));
        if ((!endpoint.IsLoopback && endpoint.Scheme != "https") || endpoint.AbsolutePath != "/" || endpoint.Query.Length != 0 || endpoint.Fragment.Length != 0)
            throw new InvalidOperationException("Relay:Endpoint must be an HTTPS origin.");
        var key = Encoding.UTF8.GetBytes(builder.Configuration["Relay:AccessKey"] ?? "");
        if (key.Length < 32) throw new InvalidOperationException("Relay:AccessKey must contain at least 32 bytes.");
        var signingKeys = new List<SecurityKey> { new SymmetricSecurityKey(key) };
        if (builder.Configuration["Relay:PreviousAccessKey"] is { Length: > 0 } previous)
        {
            var previousBytes = Encoding.UTF8.GetBytes(previous);
            if (previousBytes.Length < 32) throw new InvalidOperationException("Relay:PreviousAccessKey must contain at least 32 bytes.");
            signingKeys.Add(new SymmetricSecurityKey(previousBytes));
        }
        long outputLimit = builder.Configuration.GetValue<long?>("Relay:MaxQueuedBytes") ?? 64L * 1024 * 1024;
        if (outputLimit < 4 * 1024 * 1024 || outputLimit > int.MaxValue)
            throw new InvalidOperationException("Relay:MaxQueuedBytes must be between 4 MiB and 2 GiB minus one byte.");
        int drainSeconds = builder.Configuration.GetValue<int?>("Relay:DrainSeconds") ?? 5;
        if (drainSeconds is < 0 or > 120) throw new InvalidOperationException("Relay:DrainSeconds must be between 0 and 120.");
        builder.Services.AddSingleton<IRelayOutputBudget>(new RelayOutputBudget(outputLimit));
        builder.Services.AddSingleton<IRelayDrain>(new RelayDrain(TimeSpan.FromSeconds(drainSeconds)));
        builder.Services.AddHostedService(provider => (RelayDrain)provider.GetRequiredService<IRelayDrain>());
        if (OperatingSystem.IsLinux()) builder.Services.AddSingleton<IHostLifetime, RelayContainerLifetime>();
        builder.Services.AddConnections();
        builder.Services.AddSingleton<IRelayDirectory, RelayDirectory>();
        builder.Services.AddHostedService<RelayMaintenance>();
        builder.Services.AddAuthorization();
        var origins = builder.Configuration.GetSection("Relay:AllowedOrigins").Get<string[]>() ?? [];
        builder.Services.AddCors(options => options.AddDefaultPolicy(policy =>
        {
            if (origins.Length > 0) policy.WithOrigins(origins).AllowAnyHeader().AllowAnyMethod().AllowCredentials();
        }));
        builder.Services.AddAuthentication(JwtBearerDefaults.AuthenticationScheme).AddJwtBearer(options =>
        {
            options.MapInboundClaims = true;
            options.TokenValidationParameters = new TokenValidationParameters
            {
                ValidateIssuer = true, ValidIssuer = "azure-signalr", ValidateAudience = true, ValidateLifetime = true, ClockSkew = TimeSpan.Zero,
                IssuerSigningKeys = signingKeys, ValidateIssuerSigningKey = true,
                ValidAlgorithms = [SecurityAlgorithms.HmacSha256, SecurityAlgorithms.HmacSha512],
                AudienceValidator = (audiences, _, _) => audiences.Any(a => a.StartsWith($"{endpoint.Scheme}://{endpoint.Host}/", StringComparison.Ordinal))
            };
            options.Events = new JwtBearerEvents
            {
                OnMessageReceived = context =>
                {
                    if (context.Request.Path.StartsWithSegments("/client") && HttpMethods.IsGet(context.Request.Method))
                        context.Token = context.Request.Query["access_token"];
                    return Task.CompletedTask;
                },
                OnTokenValidated = context =>
                {
                    var request = context.Request;
                    if (request.Path.StartsWithSegments("/api/hubs"))
                    {
                        if (!context.Principal!.HasClaim("aud", $"{endpoint.Scheme}://{endpoint.Host}{request.Path.ToUriComponent()}")) context.Fail("Invalid management audience.");
                        return Task.CompletedTask;
                    }
                    var hub = request.Query["hub"];
                    var path = request.Path.StartsWithSegments("/server") ? "server" : "client";
                    if (hub.Count != 1 || string.IsNullOrWhiteSpace(hub[0]) || hub[0]!.Length > 200 ||
                        !context.Principal!.HasClaim("aud", $"{endpoint.Scheme}://{endpoint.Host}/{path}/?hub={hub[0]}"))
                        context.Fail("Invalid relay audience.");
                    return Task.CompletedTask;
                }
            };
        });
        var app = builder.Build();
        var drain = app.Services.GetRequiredService<IRelayDrain>();
        app.UseWebSockets();
        app.UseCors();
        app.UseAuthentication();
        app.UseAuthorization();
        app.MapGet("/health", () => Results.Ok());
        app.MapGet("/ready/{hub}", (HttpContext http, string hub, IRelayDirectory directory, IHostApplicationLifetime lifetime) =>
        {
            http.Response.Headers.CacheControl = "no-store";
            return !drain.IsDraining && !lifetime.ApplicationStopping.IsCancellationRequested && directory.Find(hub)?.ServerCount > 0 ? Results.Ok() : Results.StatusCode(503);
        });
        app.Use(async (http, next) =>
        {
            if (drain.IsDraining && RejectNewTransport(http))
            {
                http.Response.Headers.RetryAfter = "1";
                http.Response.StatusCode = StatusCodes.Status503ServiceUnavailable;
                return;
            }
            if (http.Request.Path.Equals("/client", StringComparison.OrdinalIgnoreCase) || http.Request.Path.Equals("/client/", StringComparison.OrdinalIgnoreCase))
            {
                var sessions = http.RequestServices.GetRequiredService<IRelayDirectory>();
                if (!sessions.CanResume(http.Request.Query["hub"].ToString(), http.Request.Query["id"].ToString(), http.User))
                {
                    http.Response.StatusCode = StatusCodes.Status403Forbidden;
                    return;
                }
            }
            if (HttpMethods.IsPost(http.Request.Method) && http.Request.Path.Equals("/client/negotiate", StringComparison.OrdinalIgnoreCase)
                && http.User.Identity?.IsAuthenticated == true)
            {
                var directory = http.RequestServices.GetRequiredService<IRelayDirectory>();
                var target = directory.Find(http.Request.Query["hub"].ToString());
                if (http.Request.Query.TryGetValue("useStatefulReconnect", out var requested))
                {
                    // The SDK redirect preserves the flag, then the client appends it again on negotiation.
                    var query = new Dictionary<string, Microsoft.Extensions.Primitives.StringValues>(http.Request.Query, StringComparer.OrdinalIgnoreCase);
                    query["useStatefulReconnect"] = target?.AllowsReconnect == true && requested.All(v => string.Equals(v, "true", StringComparison.OrdinalIgnoreCase)) ? "true" : "false";
                    http.Request.Query = new QueryCollection(query);
                }
                if (!(target?.ServerCount > 0))
                {
                    http.Response.StatusCode = StatusCodes.Status503ServiceUnavailable;
                    http.Response.Headers.RetryAfter = "1";
                    return;
                }
            }
            await next(http);
        });
        app.MapMethods("/api/health", ["GET", "HEAD"], () => Results.Ok());
        app.MapConnectionHandler<ServerHandler>("/server", options => { options.Transports = HttpTransportType.WebSockets; options.ApplicationMaxBufferSize = 2 * 1024 * 1024; }).RequireAuthorization();
        app.MapConnectionHandler<ClientHandler>("/client", options =>
        {
            options.Transports = HttpTransportType.WebSockets | HttpTransportType.ServerSentEvents | HttpTransportType.LongPolling;
            options.ApplicationMaxBufferSize = 2 * ClientFrame.MaximumLength;
            options.MinimumProtocolVersion = 1;
            options.AllowStatefulReconnects = true;
            // The client handler applies the signed identity expiry and updates its timer on refresh.
        }).RequireAuthorization();
        var management = app.MapGroup("/api/hubs/{hub}").RequireAuthorization();
        management.AddEndpointFilter(async (context, next) =>
        {
            if (context.HttpContext.Request.Query.ContainsKey("application")) return Results.BadRequest();
            return await next(context);
        });
        foreach (string route in new[] { "/:send", "/connections/{id}/:send", "/connections/{id}/:invoke", "/groups/{group}/:send", "/users/{user}/:send" })
            management.MapPost(route, ManagementSend.Handle).WithMetadata(new Microsoft.AspNetCore.Mvc.RequestSizeLimitAttribute(1024 * 1024));
        management.MapMethods("/groups/{group}", ["HEAD"], (HttpContext http, IRelayDirectory directory, string hub, string group) =>
            directory.Find(hub)?.ContainsGroup(group) == true ? Results.Ok() : Missing(http, "Warning.Group.NotExisted"));
        management.MapMethods("/users/{user}", ["HEAD"], (HttpContext http, IRelayDirectory directory, string hub, string user) =>
            directory.Find(hub)?.ContainsUser(user) == true ? Results.Ok() : Missing(http, "Warning.User.NotExisted"));
        management.MapPut("/groups/{group}/connections/{id}", (HttpContext http, IRelayDirectory directory, string hub, string group, string id) =>
            directory.Find(hub)?.JoinGroup(id, group) == true ? Results.Ok() : Missing(http, "Warning.Connection.NotExisted"));
        management.MapDelete("/groups/{group}/connections/{id}", (IRelayDirectory directory, string hub, string group, string id) =>
        {
            directory.Find(hub)?.LeaveGroup(id, group);
            return Results.Ok();
        });
        management.MapDelete("/connections/{id}/groups", (IRelayDirectory directory, string hub, string id) =>
        {
            directory.Find(hub)?.LeaveGroup(id, null);
            return Results.Ok();
        });
        management.MapMethods("/connections/{id}", ["HEAD", "DELETE"], (HttpContext http, IRelayDirectory directory, string hub, string id) =>
        {
            var target = directory.Find(hub);
            if (target is not null && (HttpMethods.IsDelete(http.Request.Method) ? target.Close(id) : target.Contains(id))) return Results.Ok();
            return Missing(http, "Warning.Connection.NotExisted");
        });
        return app;
    }

    private static bool RejectNewTransport(HttpContext http)
    {
        var path = http.Request.Path;
        if (path.StartsWithSegments("/server") || path.StartsWithSegments("/client/negotiate")) return true;
        if (!path.StartsWithSegments("/client")) return false;
        // Preserve polling, SSE posts and WebSocket resume for sessions already admitted.
        return !http.RequestServices.GetRequiredService<IRelayDirectory>().IsBound(http.Request.Query["id"].ToString());
    }

    private static IResult Missing(HttpContext http, string error)
    {
        http.Response.Headers["x-ms-error-code"] = error;
        return Results.NotFound();
    }
}
