using Microsoft.AspNetCore.Authentication.JwtBearer;
using Microsoft.AspNetCore.Http.Connections;
using Microsoft.AspNetCore.Cors.Infrastructure;
using Microsoft.Extensions.Options;

using Soenneker.SignalR.Relay.Abstract;

namespace Soenneker.SignalR.Relay;

/// <summary>Builds the HTTP and WebSocket host for the SignalR relay.</summary>
public static class RelayApplication
{
    /// <summary>Creates a host configured from command-line arguments and environment variables.</summary>
    /// <param name="args">Command-line arguments for the web host.</param>
    /// <returns>The configured relay application, ready to run.</returns>
    public static WebApplication Build(string[] args)
    {
        var builder = WebApplication.CreateBuilder(args);
        builder.Logging.AddFilter("Microsoft.AspNetCore.Hosting.Diagnostics", LogLevel.Warning); // Request URLs can carry browser bearer tokens.
        builder.Services.AddSingleton<IRelayOutputBudget, RelayOutputBudget>();
        builder.Services.AddSingleton<RelayDrain>();
        builder.Services.AddSingleton<IRelayDrain>(provider => provider.GetRequiredService<RelayDrain>());
        builder.Services.AddHostedService(provider => provider.GetRequiredService<RelayDrain>());
        if (OperatingSystem.IsLinux()) builder.Services.AddSingleton<IHostLifetime, RelayContainerLifetime>();
        builder.Services.AddConnections();
        builder.Services.AddSingleton<IRelayDirectory, RelayDirectory>();
        builder.Services.AddHostedService<RelayMaintenance>();
        builder.Services.AddAuthorization();
        builder.Services.AddCors();
        builder.Services.AddSingleton<IConfigureOptions<CorsOptions>, RelayCorsOptions>();
        builder.Services.AddAuthentication(JwtBearerDefaults.AuthenticationScheme).AddJwtBearer();
        builder.Services.AddSingleton<IConfigureOptions<JwtBearerOptions>, RelayJwtBearerOptions>();
        var app = builder.Build();
        // Validate the queue limit and signing configuration before accepting any requests.
        _ = app.Services.GetRequiredService<IRelayOutputBudget>();
        _ = app.Services.GetRequiredService<IOptionsMonitor<JwtBearerOptions>>().Get(JwtBearerDefaults.AuthenticationScheme);
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

    /// <summary>Checks whether draining should reject a transport request while bound sessions can continue.</summary>
    /// <param name="http">HTTP context for the current request.</param>
    /// <returns>True if this request would open a new transport during draining.</returns>
    private static bool RejectNewTransport(HttpContext http)
    {
        var path = http.Request.Path;
        if (path.StartsWithSegments("/server") || path.StartsWithSegments("/client/negotiate")) return true;
        if (!path.StartsWithSegments("/client")) return false;
        // Preserve polling, SSE posts and WebSocket resume for sessions already admitted.
        return !http.RequestServices.GetRequiredService<IRelayDirectory>().IsBound(http.Request.Query["id"].ToString());
    }

    /// <summary>Sets the service error code on a management response and returns HTTP 404.</summary>
    /// <param name="http">HTTP context for the current request.</param>
    /// <param name="error">Service error code to include in the response.</param>
    /// <returns>A not-found response.</returns>
    private static IResult Missing(HttpContext http, string error)
    {
        http.Response.Headers["x-ms-error-code"] = error;
        return Results.NotFound();
    }
}
