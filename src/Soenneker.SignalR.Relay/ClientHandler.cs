using Microsoft.AspNetCore.Http.Connections;
using System.Buffers;
using Microsoft.AspNetCore.Connections;
using Microsoft.AspNetCore.Connections.Features;
using Microsoft.AspNetCore.SignalR.Protocol;
using Microsoft.Azure.SignalR.Protocol;
using Microsoft.Extensions.Primitives;

using Soenneker.SignalR.Relay.Abstract;

namespace Soenneker.SignalR.Relay;

/// <summary>Admits client connections and forwards negotiated hub traffic to their assigned server.</summary>
/// <param name="directory">Hub and session directory for this relay.</param>
/// <param name="budget">Byte budget shared by peer output queues.</param>
/// <param name="drain">Coordinator for client admission and shutdown.</param>
/// <param name="lifetime">Host lifetime used to observe or request shutdown.</param>
internal sealed class ClientHandler(IRelayDirectory directory, IRelayOutputBudget budget, IRelayDrain drain, IHostApplicationLifetime lifetime) : ConnectionHandler
{
    /// <summary>Processes the client handshake, forwards frames, and releases the session on disconnect.</summary>
    /// <param name="connection">Underlying transport connection.</param>
    public override async Task OnConnectedAsync(ConnectionContext connection)
    {
        await using var admission = await drain.AdmitClientAsync();
        if (admission is null) return;
        var http = connection.GetHttpContext()!;
        var hub = directory.Find(http.Request.Query["hub"].ToString());
        if (hub is null || hub.ServerCount == 0) return;
        var peer = new RelayPeer(connection, budget);
        using var stop = CancellationTokenSource.CreateLinkedTokenSource(connection.ConnectionClosed, lifetime.ApplicationStopping);
        stop.CancelAfter(TimeSpan.FromSeconds(15));
        var output = peer.Pump(stop.Token);
        RelayClient? client = null;
        RelayIdentity? bound = null;
        RelayReplay? replay = null;
        try
        {
            while (!stop.IsCancellationRequested)
            {
                var read = await connection.Transport.Input.ReadAsync(stop.Token);
                var buffer = read.Buffer;
                if (client is null)
                {
                    var handshakeStart = buffer.Start;
                    var remaining = buffer;
                    if (remaining.Length > 4096) remaining = remaining.Slice(0, 4096);
                    if (!HandshakeProtocol.TryParseRequestMessage(ref remaining, out var handshake))
                    {
                        if (buffer.Length > 4096 || read.IsCompleted) return;
                        connection.Transport.Input.AdvanceTo(buffer.Start, buffer.End);
                        continue;
                    }
                    if (handshake.Protocol is not ("json" or "messagepack")) return;
#pragma warning disable CA2252 // The .NET 11 WebSocket dispatcher exposes transport resumption through this interface.
                    var resume = connection.Features.Get<Microsoft.AspNetCore.Connections.Abstractions.IStatefulReconnectFeature>();
                    if (resume is not null && handshake.Version >= 2 &&
                        connection.Features.Get<Microsoft.AspNetCore.Http.Connections.Features.IHttpTransportFeature>()?.TransportType == HttpTransportType.WebSockets)
                    {
                        replay = new RelayReplay(handshake.Protocol == "messagepack");
                        peer.EnableReplay(replay);
                        resume.OnReconnected(peer.Resume);
                    }
                    else resume?.DisableReconnect();
#pragma warning restore CA2252
                    var transport = connection.Features.Get<ITransferFormatFeature>()!;
                    transport.ActiveFormat = handshake.Protocol == "json" ? TransferFormat.Text : TransferFormat.Binary;
                    var claims = http.User.Claims.Where(c => c.Type is not ("aud" or "exp" or "iat" or "nbf" or "iss")).ToArray();
                    var identity = new RelayIdentity(http.Request.Query["id"].ToString(), claims, stop);
                    var expire = http.User.FindFirst("asrs.s.aeo") ?? http.User.FindFirst("exp");
                    if (expire is null || !long.TryParse(expire.Value, out long seconds)) return;
                    identity.SetExpiration(DateTimeOffset.FromUnixTimeSeconds(seconds));
                    var open = new OpenConnectionMessage(peer.Id, claims, new Dictionary<string, StringValues>(),
                        QueryString.Create("asrs.op", http.Request.Query["asrs.op"].ToString()).Value) { Protocol = handshake.Protocol };
                    if (!directory.TryBind(http.Request.Query["hub"].ToString(), identity)) return;
                    bound = identity;
                    client = hub.Attach(peer, handshake.Protocol, http.User.FindFirst("asrs.s.uid")?.Value, open, identity, replay is not null);
                    if (client is null) return;
                    var request = buffer.Slice(handshakeStart, remaining.Start);
                    if (!client.Server.TryForward(new ConnectionDataMessage(peer.Id, request))) return;
                    buffer = buffer.Slice(remaining.Start);
                }
                bool binary = client.Protocol == "messagepack";
                while (ClientFrame.TryRead(ref buffer, binary, out var frame, out var body))
                {
                    if (replay is not null && !replay.Accept(body, peer)) continue;
                    if (client.HasOutstandingInvocations && ClientFrame.CompletionId(body, binary) is { } id)
                    {
                        if (hub.CompleteManaged(client, id, frame, body)) continue;
                        hub.RememberCompletion(client, id, frame);
                    }
                    if (!client.Server.TryForward(new ConnectionDataMessage(peer.Id, frame))) return;
                }
                connection.Transport.Input.AdvanceTo(buffer.Start, buffer.End);
                if (read.IsCompleted || read.IsCanceled) break;
            }
        }
        catch (OperationCanceledException) when (stop.IsCancellationRequested) { }
        finally
        {
            if (client is not null) hub.Detach(client);
            if (bound is not null) directory.Unbind(bound);
            stop.Cancel();
            try { await output; } catch (OperationCanceledException) when (stop.IsCancellationRequested) { }
            finally { replay?.Dispose(); }
        }
    }
}
