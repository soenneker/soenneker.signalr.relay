using Microsoft.AspNetCore.Connections.Features;
using Microsoft.AspNetCore.Http.Connections;
using Microsoft.AspNetCore.Connections;
using Microsoft.Azure.SignalR.Protocol;

using Soenneker.SignalR.Relay.Abstract;

namespace Soenneker.SignalR.Relay;

/// <summary>Handles service-protocol tunnels from upstream application servers.</summary>
/// <param name="directory">Hub and session directory for this relay.</param>
/// <param name="budget">Byte budget shared by peer output queues.</param>
/// <param name="drain">Coordinator for client admission and shutdown.</param>
/// <param name="lifetime">Host lifetime used to observe or request shutdown.</param>
internal sealed class ServerHandler(IRelayDirectory directory, IRelayOutputBudget budget, IRelayDrain drain, IHostApplicationLifetime lifetime) : ConnectionHandler
{
    /// <summary>Maximum incoming service frame size in bytes.</summary>
    private const int MaxFrameBytes = 1024 * 1024;

    /// <summary>Service protocol codec used for tunnel framing.</summary>
    private static readonly ServiceProtocol Protocol = new();

    /// <summary>Negotiates the server tunnel and dispatches incoming service messages until disconnect.</summary>
    /// <param name="connection">Underlying transport connection.</param>
    public override async Task OnConnectedAsync(ConnectionContext connection)
    {
        if (drain.IsDraining) return;
        connection.Features.Get<ITransferFormatFeature>()!.ActiveFormat = TransferFormat.Binary;
        var http = connection.GetHttpContext()!;
        var hub = directory.Get(http.Request.Query["hub"].ToString());
        var peer = new RelayPeer(connection, budget, http.Request.Headers["X-ASRS-Server-Id"].ToString());
        using var stop = CancellationTokenSource.CreateLinkedTokenSource(connection.ConnectionClosed, lifetime.ApplicationStopping);
        stop.CancelAfter(TimeSpan.FromSeconds(15));
        var output = peer.Pump(stop.Token);
        Task heartbeat = Task.CompletedTask;
        bool initialized = false;
        try
        {
            while (!stop.IsCancellationRequested)
            {
                var read = await connection.Transport.Input.ReadAsync(stop.Token);
                var buffer = read.Buffer;
                while (!buffer.IsEmpty)
                {
                    // A pipe read can contain several valid frames. Enforce the bound on each encoded frame.
                    var frame = buffer.Slice(0, Math.Min(buffer.Length, MaxFrameBytes));
                    if (!Protocol.TryParseMessage(ref frame, out var message))
                    {
                        if (buffer.Length >= MaxFrameBytes) throw new InvalidDataException("Service frame too large.");
                        break;
                    }
                    buffer = buffer.Slice(frame.Start);
                    if (!initialized)
                    {
                        if (message is not HandshakeRequestMessage handshake || handshake.Version != Protocol.Version ||
                            handshake.ConnectionType is < 0 or > 2) return;
                        peer.AllowsReconnect = handshake.AllowStatefulReconnects;
                        peer.Send(new HandshakeResponseMessage());
                        if (handshake.ConnectionType is 0 or 1) hub.Register(peer);
                        initialized = true;
                        heartbeat = Heartbeat(peer, stop.Token);
                    }
                    else if (message is not null) hub.Dispatch(peer, message);
                    // The SDK sends a keepalive every five seconds. Partial frames do not renew a dead tunnel.
                    stop.CancelAfter(TimeSpan.FromSeconds(30));
                }
                connection.Transport.Input.AdvanceTo(buffer.Start, buffer.End);
                if (read.IsCompleted || read.IsCanceled) break;
            }
        }
        catch (OperationCanceledException) when (stop.IsCancellationRequested) { }
        finally
        {
            hub.Remove(peer);
            stop.Cancel();
            try { await Task.WhenAll(output, heartbeat); } catch (OperationCanceledException) when (stop.IsCancellationRequested) { }
        }
    }

    /// <summary>Sends periodic service pings until the tunnel closes.</summary>
    /// <param name="peer">Transport peer for this connection.</param>
    /// <param name="token">Cancels the transport loop.</param>
    private static async Task Heartbeat(RelayPeer peer, CancellationToken token)
    {
        using var timer = new PeriodicTimer(TimeSpan.FromSeconds(5));
        while (await timer.WaitForNextTickAsync(token)) peer.Send(PingMessage.Instance);
    }
}
