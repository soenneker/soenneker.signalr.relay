using System;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Connections;
using Microsoft.Azure.SignalR.Protocol;

namespace Soenneker.Cloudflare.SignalR.Tests;

public sealed class RelayBufferTests
{
    [Test]
    public async Task BlockedTransportRemainsChargedUntilWriteIsCanceled()
    {
        var budget = new RelayOutputBudget(1024);
        var pipe = new System.IO.Pipelines.Pipe(new System.IO.Pipelines.PipeOptions(pauseWriterThreshold: 1, resumeWriterThreshold: 1));
        await using var connection = new DefaultConnectionContext("slow") { Transport = new TestDuplexPipe(pipe.Reader, pipe.Writer) };
        using var timeout = new System.Threading.CancellationTokenSource(TimeSpan.FromSeconds(5));
        var peer = new RelayPeer(connection, budget);
        var pump = peer.Pump(timeout.Token);
        peer.Send(new byte[400]);
        var read = await pipe.Reader.ReadAsync(timeout.Token);
        peer.Send(new byte[300]);
        peer.Abort();
        await Assert.That(budget.Used).IsEqualTo(400);
        timeout.Cancel();
        try { await pump; } catch (OperationCanceledException) when (timeout.IsCancellationRequested) { }
        await Assert.That(budget.Used).IsEqualTo(0);
        pipe.Reader.AdvanceTo(read.Buffer.End);
        await pipe.Reader.CompleteAsync();
        await pipe.Writer.CompleteAsync();
        await WaitForAbort(connection);
    }

    [Test]
    public async Task PressureEvictsBackloggedClientAndReleasesAllReservations()
    {
        var budget = new RelayOutputBudget(1024);
        await using var slowConnection = new DefaultConnectionContext("slow");
        await using var freshConnection = new DefaultConnectionContext("fresh");
        await using var tunnelConnection = new DefaultConnectionContext("server");
        var slow = new RelayPeer(slowConnection, budget);
        var fresh = new RelayPeer(freshConnection, budget);
        var tunnel = new RelayPeer(tunnelConnection, budget, "api");
        slow.Send(new byte[800]);
        tunnel.Send(new byte[100]);
        await Assert.That(budget.Used).IsEqualTo(900);
        fresh.Send(new byte[128]);
        await Assert.That(budget.Used).IsEqualTo(228);
        await Assert.That(slow.PendingBytes).IsEqualTo(0);
        await Assert.That(fresh.PendingBytes).IsEqualTo(128);
        await Assert.That(tunnel.PendingBytes).IsEqualTo(100);
        slow.Send(new byte[10]);
        fresh.Abort();
        tunnel.Abort();
        await Assert.That(budget.Used).IsEqualTo(0);
        await Task.WhenAll(WaitForAbort(slowConnection), WaitForAbort(freshConnection), WaitForAbort(tunnelConnection));
    }

    [Test]
    public async Task BusyClientLeavesRoomForControlFramesOnItsServerTunnel()
    {
        var transport = new DefaultConnectionContext("test-server");
        var peer = new RelayPeer(transport, new RelayOutputBudget(64 << 20));
        var message = new ConnectionDataMessage("client", new byte[64 * 1024]);
        var count = 0;
        while (peer.TryForward(message)) count++;
        await Assert.That(count is > 0 and < 64).IsTrue();
        peer.Send(new CloseConnectionMessage("client"));
        await Assert.That(transport.ConnectionClosed.IsCancellationRequested).IsFalse();
        await transport.DisposeAsync();
    }

    [Test]
    public async Task DrainingTunnelFinishesExistingWorkAndRefusesNewClients()
    {
        await using var server = new DefaultConnectionContext("server");
        await using var connection = new DefaultConnectionContext("client");
        using var lifetime = new System.Threading.CancellationTokenSource();
        var peer = new RelayPeer(server, new RelayOutputBudget(64 << 20), "api");
        var receiver = new RelayPeer(connection, new RelayOutputBudget(64 << 20));
        var hub = new RelayHub();
        hub.Register(peer);
        var open = new OpenConnectionMessage(receiver.Id, []);
        var identity = new RelayIdentity("token", [], lifetime);
        var attached = hub.Attach(receiver, "json", "user", open, identity);
        await Assert.That(attached).IsNotNull();
        hub.Dispatch(peer, new PingMessage { Messages = ["shutdown", "fin:2"] });
        await Assert.That(hub.ServerCount).IsEqualTo(0);
        await Assert.That(hub.Contains(receiver.Id)).IsTrue();
        await Assert.That(hub.JoinGroup(receiver.Id, "existing")).IsTrue();
        await Assert.That(hub.Attach(receiver, "json", "user", open, identity)).IsNull();
        hub.Remove(peer);
        await Assert.That(hub.Contains(receiver.Id)).IsFalse();
        await WaitForAbort(connection);
    }

    [Test]
    public async Task SubscriberFiltersRejectUnknownSyntaxAndRespectAudience()
    {
        await using var transport = new DefaultConnectionContext("c1");
        var peer = new RelayPeer(transport, new RelayOutputBudget(64 << 20));
        using var stop = new System.Threading.CancellationTokenSource();
        var client = new RelayClient(peer, peer, "json", "listener-O'Brien", new RelayIdentity("", [], stop));
        var filter = SubscriberFilter.Parse("startswith(userId, 'listener-') and (connectionId eq 'c1' or endswith(userId, 'other'))");
        await Assert.That(filter!(client)).IsTrue();
        await Assert.That(SubscriberFilter.Parse("not (userId eq 'listener-O''Brien')")!(client)).IsFalse();
        await Assert.That(SubscriberFilter.Parse("unknown eq 'v'")).IsNull();
        await Assert.That(SubscriberFilter.Parse("userId ne 'x' trailing")).IsNull();
    }

    [Test]
    public async Task UserGrantExpiryPreservesIndependentConnectionMembership()
    {
        var users = new RelayAudienceIndex();
        var groups = new RelayAudienceIndex();
        var grants = new RelayUserGroups(users, groups);
        await using var transport = new DefaultConnectionContext("session");
        var peer = new RelayPeer(transport, new RelayOutputBudget(64 << 20));
        using var stop = new System.Threading.CancellationTokenSource();
        var client = new RelayClient(peer, peer, "json", "reader", new RelayIdentity("", [], stop));
        grants.Join("reader", "inherited", 5);
        grants.Join("reader", "both", 5);
        users.Add("reader", client);
        grants.Attach(client, "reader");
        client.Groups["both"] = 0;
        await Assert.That(groups.Contains("inherited")).IsTrue();
        grants.Expire(DateTimeOffset.UtcNow.AddSeconds(6));
        await Assert.That(groups.Contains("inherited")).IsFalse();
        await Assert.That(groups.Contains("both")).IsTrue();
    }

    [Test]
    public async Task ManagementKeysRejectConflictsAndExpire()
    {
        var hub = new RelayHub();
        var key = Guid.NewGuid();
        int Send(string fingerprint) => hub.SendManaged(null, "subscribers", null, [], [], default, key, fingerprint);
        await Assert.That(Send("one")).IsEqualTo(202);
        await Assert.That(Send("one")).IsEqualTo(202);
        await Assert.That(Send("two")).IsEqualTo(409);
        hub.Maintain(DateTimeOffset.UtcNow.AddMinutes(6));
        await Assert.That(Send("two")).IsEqualTo(202);
    }

    [Test]
    public async Task FullOutputQueueStopsAcceptingClientMessages()
    {
        var transport = new DefaultConnectionContext("test-client");
        var peer = new RelayPeer(transport, new RelayOutputBudget(64 << 20));
        var data = new byte[1024 * 1024];
        for (int i = 0; i < 5; i++) peer.Send(data);
        await Assert.That(peer.TryForward(new ConnectionDataMessage("id", new byte[1]))).IsFalse();
        await WaitForAbort(transport);
        await transport.DisposeAsync();
    }

    private static async Task WaitForAbort(DefaultConnectionContext transport)
    {
        var closed = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        using var callback = transport.ConnectionClosed.Register(() => closed.TrySetResult());
        await closed.Task.WaitAsync(TimeSpan.FromSeconds(5));
    }
}
