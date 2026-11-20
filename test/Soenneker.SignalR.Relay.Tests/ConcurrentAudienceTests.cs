using System;
using System.Collections.Concurrent;
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

using Soenneker.SignalR.Relay.Abstract;

namespace Soenneker.SignalR.Relay.Tests;

public sealed class ConcurrentAudienceTests
{
    [Test]
    public async Task GroupDeliveryIsIsolatedAcrossConcurrentMembershipChanges()
    {
        const int count = 96;
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(55));
        var token = timeout.Token;
        using var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        var endpoint = $"http://127.0.0.1:{((IPEndPoint)listener.LocalEndpoint).Port}";
        listener.Stop();
        string key = Convert.ToBase64String(RandomNumberGenerator.GetBytes(48));
        await using var relay = RelayApplication.Build([$"--Relay:Endpoint={endpoint}", $"--Relay:AccessKey={key}", $"--urls={endpoint}"]);
        await relay.StartAsync(token);
        var builder = WebApplication.CreateBuilder();
        builder.WebHost.UseUrls("http://127.0.0.1:0");
        builder.Services.AddSignalR().AddAzureSignalR(o =>
        {
            o.ConnectionString = $"Endpoint={endpoint};AccessKey={key};";
            o.InitialHubServerConnectionCount = 3;
        });
        await using var api = builder.Build();
        api.MapHub<RoutingHub>("/app");
        await api.StartAsync(token);
        var clients = Enumerable.Range(0, count).Select(_ => new HubConnectionBuilder().WithUrl(api.Urls.Single() + "/app").Build()).ToArray();
        var deliveries = new ConcurrentQueue<int>[count];
        try
        {
            while (relay.Services.GetRequiredService<IRelayDirectory>().Find("routinghub")?.ServerCount != 3)
                await Task.Delay(20, token);
            await Parallel.ForEachAsync(Enumerable.Range(0, count), new ParallelOptions { MaxDegreeOfParallelism = 8, CancellationToken = token },
                async (i, ct) =>
                {
                    var messages = deliveries[i] = new ConcurrentQueue<int>();
                    clients[i].On<int>("Delivery", messages.Enqueue);
                    clients[i].On("Barrier", () => true);
                    await clients[i].StartAsync(ct);
                    await clients[i].InvokeAsync("Join", $"audience-{i % 8}", ct);
                    if (i % 3 == 0)
                    {
                        await clients[i].InvokeAsync("Leave", $"audience-{i % 8}", ct);
                        await clients[i].InvokeAsync("Join", "moved", ct);
                    }
                });
            var hub = api.Services.GetRequiredService<IHubContext<RoutingHub>>();
            for (int group = 0; group < 8; group++)
                await hub.Clients.Group($"audience-{group}").SendAsync("Delivery", group, token);
            await hub.Clients.Groups(["moved", "moved"]).SendAsync("Delivery", 8, token);
            // Wait for the expected delivery before the barrier to allow independent server tunnels to progress.
            while (deliveries.Any(q => q.IsEmpty)) await Task.Delay(10, token);
            for (int i = 0; i < count; i++)
            {
                await hub.Clients.Client(clients[i].ConnectionId!).InvokeAsync<bool>("Barrier", token);
            }
            for (int i = 0; i < count; i++)
                await Assert.That(deliveries[i].ToArray().SequenceEqual([i % 3 == 0 ? 8 : i % 8])).IsTrue();
        }
        finally
        {
            await Task.WhenAll(clients.Select(client => client.DisposeAsync().AsTask()));
            await api.StopAsync();
            await relay.StopAsync();
        }
    }
}
