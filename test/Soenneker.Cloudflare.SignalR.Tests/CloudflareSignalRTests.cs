using System;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Security.Cryptography;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.SignalR;
using Microsoft.AspNetCore.SignalR.Client;
using Microsoft.Extensions.DependencyInjection;

namespace Soenneker.Cloudflare.SignalR.Tests;

public sealed class CloudflareSignalRTests
{
    [Test]
    [Arguments(Microsoft.AspNetCore.Http.Connections.HttpTransportType.WebSockets, false)]
    [Arguments(Microsoft.AspNetCore.Http.Connections.HttpTransportType.WebSockets, true)]
    [Arguments(Microsoft.AspNetCore.Http.Connections.HttpTransportType.ServerSentEvents, false)]
    [Arguments(Microsoft.AspNetCore.Http.Connections.HttpTransportType.LongPolling, false)]
    public async Task IndependentApiInstancesDeliverGroupMessages(Microsoft.AspNetCore.Http.Connections.HttpTransportType transport, bool binary)
    {
        using var deadline = new CancellationTokenSource(TimeSpan.FromSeconds(25));
        var token = deadline.Token;
        using var port = new TcpListener(IPAddress.Loopback, 0);
        port.Start();
        var endpoint = $"http://127.0.0.1:{((IPEndPoint)port.LocalEndpoint).Port}";
        port.Stop();
        string key = Convert.ToBase64String(RandomNumberGenerator.GetBytes(48));
        await using var relay = RelayApplication.Build([$"--Relay:Endpoint={endpoint}", $"--Relay:AccessKey={key}", $"--urls={endpoint}"]);
        await relay.StartAsync(token);
        var connectionString = $"Endpoint={endpoint};AccessKey={key};Version=1.0;";
        await using var first = await StartApi(connectionString, token);

        var clientBuilder = new HubConnectionBuilder().WithUrl(first.Urls.Single() + "/app", transport);
        if (binary) clientBuilder.AddMessagePackProtocol();
        await using var client = clientBuilder.Build();
        var received = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);
        client.On("ReadVersion", () => 7);
        client.On<string>("Changed", value => received.TrySetResult(value));
        try
        {
            while (relay.Services.GetRequiredService<IRelayDirectory>().Get("routinghub").ServerCount == 0) await Task.Delay(20, token);
            using var http = new System.Net.Http.HttpClient();
            using var unauthorized = await http.PostAsync(endpoint + "/client/negotiate?hub=routinghub", null, token);
            await Assert.That(unauthorized.StatusCode).IsEqualTo(HttpStatusCode.Unauthorized);
            await client.StartAsync(token);
            await client.InvokeAsync("Subscribe", token);
            await using var second = await StartApi(connectionString, token);
            while (relay.Services.GetRequiredService<IRelayDirectory>().Get("routinghub").ServerCount < 2) await Task.Delay(20, token);
            await second.Services.GetRequiredService<IHubContext<RoutingHub>>().Clients.Group("subscribers").SendAsync("Changed", "updated", token);
            await Assert.That(await received.Task.WaitAsync(token)).IsEqualTo("updated");

            var manager = new Microsoft.Azure.SignalR.Management.ServiceManagerBuilder()
                .WithOptions(options => options.ConnectionString = connectionString).BuildServiceManager();
            await using var management = await manager.CreateHubContextAsync(nameof(RoutingHub), token);
            var id = client.ConnectionId!;
            var pushed = new TaskCompletionSource<int>(TaskCreationOptions.RunContinuationsAsynchronously);
            client.On<int>("GroupUpdate", value => pushed.TrySetResult(value));
            await management.Groups.AddToGroupAsync(id, "managed", token);
            await management.Clients.Group("managed").SendAsync("GroupUpdate", 42, token);
            await Assert.That(await pushed.Task.WaitAsync(token)).IsEqualTo(42);
            await management.Groups.RemoveFromGroupAsync(id, "managed", token);
            await Assert.That(relay.Services.GetRequiredService<IRelayDirectory>().Get("routinghub").ContainsGroup("managed")).IsFalse();
            var invokePath = "/api/hubs/routinghub/connections/" + id + "/:invoke";
            var operation = Guid.NewGuid();
            int executions = 0;
            client.On("Increment", () => Interlocked.Increment(ref executions));
            async Task<System.Net.HttpStatusCode> InvokeOnce(string method)
            {
                var jwt = new Microsoft.IdentityModel.JsonWebTokens.JsonWebTokenHandler().CreateToken(new Microsoft.IdentityModel.Tokens.SecurityTokenDescriptor
                {
                    Issuer = "azure-signalr", Audience = "http://127.0.0.1" + invokePath, Expires = DateTime.UtcNow.AddMinutes(1),
                    SigningCredentials = new Microsoft.IdentityModel.Tokens.SigningCredentials(
                        new Microsoft.IdentityModel.Tokens.SymmetricSecurityKey(System.Text.Encoding.UTF8.GetBytes(key)), "HS256")
                });
                using var request = new System.Net.Http.HttpRequestMessage(System.Net.Http.HttpMethod.Post, endpoint + invokePath);
                request.Headers.Authorization = new System.Net.Http.Headers.AuthenticationHeaderValue("Bearer", jwt);
                request.Headers.Add("Idempotency-Key", operation.ToString());
                request.Content = System.Net.Http.Json.JsonContent.Create(new { target = method, arguments = Array.Empty<object>() });
                using var response = await http.SendAsync(request, token);
                return response.StatusCode;
            }
            var replies = await Task.WhenAll(InvokeOnce("Increment"), InvokeOnce("Increment"));
            await Assert.That(replies.All(status => status == HttpStatusCode.OK) && executions == 1).IsTrue();
            await Assert.That(await InvokeOnce("AnotherMethod")).IsEqualTo(HttpStatusCode.Conflict);
            using var persistentManager = new Microsoft.Azure.SignalR.Management.ServiceManagerBuilder().WithOptions(o =>
            {
                o.ConnectionString = connectionString;
                o.ServiceTransportType = Microsoft.Azure.SignalR.Management.ServiceTransportType.Persistent;
            }).BuildServiceManager();
            await using var persistent = await persistentManager.CreateHubContextAsync(nameof(RoutingHub), token);
            await persistent.UserGroups.AddToGroupAsync("offline-user", "pending", TimeSpan.FromSeconds(10), token);
            await Assert.That(await persistent.UserGroups.IsUserInGroup("offline-user", "pending", token)).IsTrue();
            await persistent.UserGroups.RemoveFromAllGroupsAsync("offline-user", token);
            await Assert.That(await persistent.UserGroups.IsUserInGroup("offline-user", "pending", token)).IsFalse();
            var listed = new System.Collections.Generic.List<string>();
            await foreach (var member in persistent.Groups.ListConnectionsInGroup("subscribers", cancellationToken: token))
                listed.Add(member.ConnectionId);
            await Assert.That(listed.Single()).IsEqualTo(id);
            var managedResult = await management.Clients.Client(id).InvokeAsync<int>("ReadVersion", token);
            await Assert.That(managedResult).IsEqualTo(7);
            var version = await second.Services.GetRequiredService<IHubContext<RoutingHub>>().Clients.Client(id).InvokeAsync<int>("ReadVersion", token);
            await Assert.That(version).IsEqualTo(7);
            await Assert.That(await management.ClientManager.ConnectionExistsAsync(id, token)).IsTrue();
            var closed = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            client.Closed += _ => { closed.TrySetResult(); return Task.CompletedTask; };
            await management.ClientManager.CloseConnectionAsync(id, "Membership revoked", token);
            await closed.Task.WaitAsync(token);
            await Assert.That(await management.ClientManager.ConnectionExistsAsync(id, token)).IsFalse();
            await second.StopAsync();
        }
        finally
        {
            await client.StopAsync();

            await first.StopAsync();
            await relay.StopAsync();
        }
    }

    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task ResumeWebSocketWithoutRejoining(bool binary)
    {
        using var deadline = new CancellationTokenSource(TimeSpan.FromSeconds(15));
        var token = deadline.Token;
        using var port = new TcpListener(IPAddress.Loopback, 0);
        port.Start();
        var endpoint = $"http://127.0.0.1:{((IPEndPoint)port.LocalEndpoint).Port}";
        port.Stop();
        var key = Convert.ToBase64String(RandomNumberGenerator.GetBytes(48));
        await using var relay = RelayApplication.Build([$"--Relay:Endpoint={endpoint}", $"--Relay:AccessKey={key}", $"--urls={endpoint}"]);
        await relay.StartAsync(token);
        await using var api = await StartApi($"Endpoint={endpoint};AccessKey={key};", token);
        System.Net.WebSockets.ClientWebSocket? socket = null;
        var reconnectStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var builder = new HubConnectionBuilder().WithUrl(api.Urls.Single() + "/app", options =>
        {
            options.Transports = Microsoft.AspNetCore.Http.Connections.HttpTransportType.WebSockets;
            options.WebSocketFactory = async (context, cancel) =>
            {
                if (socket is not null)
                {
                    reconnectStarted.TrySetResult();
                    await release.Task.WaitAsync(token);
                }
                socket = new System.Net.WebSockets.ClientWebSocket();
                socket.Options.SetRequestHeader("Authorization", "Bearer " + await context.Options.AccessTokenProvider!());
                await socket.ConnectAsync(context.Uri, cancel);
                return socket;
            };
        }).WithStatefulReconnect();
        if (binary) builder.AddMessagePackProtocol();
        await using var client = builder.Build();
        var messages = System.Threading.Channels.Channel.CreateUnbounded<int>();
        client.On<int>("Update", value => { messages.Writer.TryWrite(value); });
        try
        {
            while (relay.Services.GetRequiredService<IRelayDirectory>().Get("routinghub").ServerCount == 0) await Task.Delay(20, token);
            await client.StartAsync(token);
            await client.InvokeAsync("Subscribe", token);
            var id = client.ConnectionId;
            socket!.Abort();
            await reconnectStarted.Task.WaitAsync(token);
            var group = api.Services.GetRequiredService<IHubContext<RoutingHub>>().Clients.Group("subscribers");
            await group.SendAsync("Update", 1, token);
            await group.SendAsync("Update", 2, token);
            release.TrySetResult();
            await Assert.That(await messages.Reader.ReadAsync(token)).IsEqualTo(1);
            await Assert.That(await messages.Reader.ReadAsync(token)).IsEqualTo(2);
            await client.InvokeAsync("Subscribe", token);
            await group.SendAsync("Update", 3, token);
            await Assert.That(await messages.Reader.ReadAsync(token)).IsEqualTo(3);
            await Assert.That(client.ConnectionId).IsEqualTo(id);
        }
        finally
        {
            release.TrySetResult();
            await client.StopAsync();
            await api.StopAsync();
            await relay.StopAsync();
        }
    }

    [Test]
    public async Task ShutdownRejectsNegotiationAndFinishesInFlightInvocation()
    {
        using var deadline = new CancellationTokenSource(TimeSpan.FromSeconds(25));
        var token = deadline.Token;
        using var port = new TcpListener(IPAddress.Loopback, 0);
        port.Start();
        var endpoint = $"http://127.0.0.1:{((IPEndPoint)port.LocalEndpoint).Port}";
        port.Stop();
        var key = Convert.ToBase64String(RandomNumberGenerator.GetBytes(48));
        await using var relay = RelayApplication.Build([$"--Relay:Endpoint={endpoint}", $"--Relay:AccessKey={key}", $"--urls={endpoint}", "--Relay:DrainSeconds=3"]);
        await relay.StartAsync(token);
        await using var api = await StartApi($"Endpoint={endpoint};AccessKey={key};", token);
        await using var client = new HubConnectionBuilder().WithUrl(api.Urls.Single() + "/app").Build();
        var started = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource<int>(TaskCreationOptions.RunContinuationsAsynchronously);
        var disconnected = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        client.On("Hold", () => { started.TrySetResult(); return release.Task; });
        client.Closed += _ => { disconnected.TrySetResult(); return Task.CompletedTask; };
        try
        {
            using var http = new System.Net.Http.HttpClient();
            while (!(relay.Services.GetRequiredService<IRelayDirectory>().Find("routinghub")?.ServerCount > 0)) await Task.Delay(20, token);
            await client.StartAsync(token);
            var pending = api.Services.GetRequiredService<IHubContext<RoutingHub>>().Clients.Client(client.ConnectionId!).InvokeAsync<int>("Hold", token);
            await started.Task.WaitAsync(token);
            var stopping = relay.StopAsync(token);
            using var readiness = await http.GetAsync(endpoint + "/ready/routinghub", token);
            await Assert.That(readiness.StatusCode).IsEqualTo(HttpStatusCode.ServiceUnavailable);
            await using var newcomer = new HubConnectionBuilder().WithUrl(api.Urls.Single() + "/app").Build();
            try
            {
                await newcomer.StartAsync(token);
                throw new InvalidOperationException("Relay admitted a new connection during shutdown.");
            }
            catch (System.Net.Http.HttpRequestException e) when (e.StatusCode == HttpStatusCode.ServiceUnavailable) { }
            release.TrySetResult(42);
            await Assert.That(await pending.WaitAsync(token)).IsEqualTo(42);
            await stopping.WaitAsync(token);
            await disconnected.Task.WaitAsync(token);
        }
        finally
        {
            release.TrySetCanceled();
            await client.StopAsync();
            await api.StopAsync();
            await relay.StopAsync();
        }
    }

    private static async Task<WebApplication> StartApi(string connectionString, CancellationToken token)
    {
        var builder = WebApplication.CreateBuilder();
        builder.WebHost.UseUrls("http://127.0.0.1:0");
        builder.Services.AddSignalR().AddMessagePackProtocol().AddAzureSignalR(options =>
        {
            options.AllowStatefulReconnects = true;
            options.ConnectionString = connectionString;
            options.InitialHubServerConnectionCount = 1;
        });
        var app = builder.Build();
        app.MapHub<RoutingHub>("/app");
        await app.StartAsync(token);
        return app;
    }
}
