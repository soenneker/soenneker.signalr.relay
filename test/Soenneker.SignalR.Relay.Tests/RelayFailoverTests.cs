using System;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Net.Sockets;
using System.Security.Cryptography;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.SignalR;
using Microsoft.AspNetCore.SignalR.Client;
using Microsoft.Azure.SignalR;
using Microsoft.Extensions.DependencyInjection;

using Soenneker.SignalR.Relay.Abstract;

namespace Soenneker.SignalR.Relay.Tests;

public sealed class RelayFailoverTests
{
    [Test]
    public async Task ClientReconnectsThroughSurvivingRelayAndRestoresGroupDelivery()
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(55));
        var token = timeout.Token;
        var primaryAddress = LocalEndpoint("127.0.0.1");
        var standbyAddress = LocalEndpoint("localhost");
        var primaryKey = Convert.ToBase64String(RandomNumberGenerator.GetBytes(48));
        var standbyKey = Convert.ToBase64String(RandomNumberGenerator.GetBytes(48));
        await using var primary = RelayApplication.Build([$"--Relay:Endpoint={primaryAddress}", $"--Relay:AccessKey={primaryKey}", $"--urls={primaryAddress}"]);
        await using var standby = RelayApplication.Build([$"--Relay:Endpoint={standbyAddress}", $"--Relay:AccessKey={standbyKey}", $"--urls={standbyAddress}"]);
        await primary.StartAsync(token);
        await standby.StartAsync(token);
        using var http = new HttpClient();
        using var cold = await http.GetAsync(primaryAddress + "/ready/routinghub", token);
        await Assert.That(cold.StatusCode).IsEqualTo(HttpStatusCode.ServiceUnavailable);
        var builder = WebApplication.CreateBuilder();
        builder.WebHost.UseUrls("http://127.0.0.1:0");
        builder.Services.AddSignalR().AddAzureSignalR(options =>
        {
            options.Endpoints = [
                new ServiceEndpoint($"Endpoint={primaryAddress};AccessKey={primaryKey};"),
                new ServiceEndpoint($"Endpoint={standbyAddress};AccessKey={standbyKey};" )
            ];
            options.InitialHubServerConnectionCount = 1;
        });
        await using var api = builder.Build();
        api.MapHub<RoutingHub>("/app");
        await api.StartAsync(token);
        await using var client = new HubConnectionBuilder().WithUrl(api.Urls.Single() + "/app")
            .WithAutomaticReconnect([TimeSpan.Zero, TimeSpan.FromSeconds(1), TimeSpan.FromSeconds(2), TimeSpan.FromSeconds(5)]).Build();
        try
        {
            await WaitUntilReady(http, primaryAddress, token);
            await WaitUntilReady(http, standbyAddress, token);
            var restored = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            client.Reconnected += async _ =>
            {
                try { await client.InvokeAsync("Subscribe", token); restored.TrySetResult(); }
                catch (Exception e) { restored.TrySetException(e); }
            };
            var received = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);
            client.On<string>("Changed", value => { received.TrySetResult(value); });
            client.On("ReadVersion", () => 7);
            await client.StartAsync(token);
            await client.InvokeAsync("Subscribe", token);
            var result = await api.Services.GetRequiredService<IHubContext<RoutingHub>>().Clients.Client(client.ConnectionId!).InvokeAsync<int>("ReadVersion", token);
            await Assert.That(result).IsEqualTo(7);
            var onPrimary = primary.Services.GetRequiredService<IRelayDirectory>().Find("routinghub")!.Contains(client.ConnectionId!);
            var active = onPrimary ? primary : standby;
            var survivor = onPrimary ? standby : primary;
            await active.StopAsync(token);
            await restored.Task.WaitAsync(token);
            await Assert.That(survivor.Services.GetRequiredService<IRelayDirectory>().Find("routinghub")!.Contains(client.ConnectionId!)).IsTrue();
            await api.Services.GetRequiredService<IHubContext<RoutingHub>>().Clients.Group("subscribers").SendAsync("Changed", "after failover", token);
            await Assert.That(await received.Task.WaitAsync(token)).IsEqualTo("after failover");
        }
        finally
        {
            await client.StopAsync();
            await api.StopAsync();
            await primary.StopAsync();
            await standby.StopAsync();
        }
    }

    private static string LocalEndpoint(string host)
    {
        using var port = new TcpListener(IPAddress.Loopback, 0);
        port.Start();
        return $"http://{host}:{((IPEndPoint)port.LocalEndpoint).Port}";
    }

    private static async Task WaitUntilReady(HttpClient http, string origin, CancellationToken token)
    {
        while (true)
        {
            using var ready = await http.GetAsync(origin + "/ready/routinghub", token);
            if (ready.IsSuccessStatusCode) return;
            await Task.Delay(25, token);
        }
    }
}
