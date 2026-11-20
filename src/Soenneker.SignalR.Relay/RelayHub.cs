using System.Buffers;
using System.Collections.Concurrent;
using Microsoft.Azure.SignalR.Protocol;
using Soenneker.SignalR.Relay.Dtos;
using Soenneker.Utils.Random;

namespace Soenneker.SignalR.Relay;

/// <summary>Routes client and server traffic, group membership, and invocation completions for one hub.</summary>
internal sealed class RelayHub
{
    /// <summary>Connections indexed by group name.</summary>
    private readonly RelayAudienceIndex _groups = new();

    /// <summary>Connections indexed by user identifier.</summary>
    private readonly RelayAudienceIndex _users = new();

    /// <summary>User-level grants applied to current and future connections.</summary>
    private readonly RelayUserGroups _userGroups;

    /// <summary>Creates an empty hub with shared user and group indexes.</summary>
    public RelayHub() => _userGroups = new RelayUserGroups(_users, _groups);

    /// <summary>Attached clients indexed by nonempty session token.</summary>
    private readonly Dictionary<string, RelayClient> _tokens = new(StringComparer.Ordinal);

    /// <summary>Server tunnels available for new connections.</summary>
    private readonly List<RelayPeer> _servers = [];

    /// <summary>Live clients keyed by connection identifier.</summary>
    private readonly ConcurrentDictionary<string, RelayClient> _clients = new(StringComparer.Ordinal);

    /// <summary>Server-originated invocations awaiting completion or recovery.</summary>
    private readonly Dictionary<(string ClientId, string InvocationId), PendingInvocation> _invocations = [];

    /// <summary>Fingerprints of accepted management sends indexed by idempotency key.</summary>
    private readonly Dictionary<Guid, string> _sentOperations = [];

    /// <summary>Expiration order for retained management send keys.</summary>
    private readonly Queue<(Guid Id, DateTimeOffset Expires)> _sentExpiry = [];

    /// <summary>Total bytes of captured completions retained for server-side reply recovery.</summary>
    private int _replyBytes;

    /// <summary>Serializes hub membership, routing snapshots, and invocation bookkeeping.</summary>
    private readonly Lock _gate = new();

    /// <summary>Gets whether any available server tunnel supports stateful reconnect.</summary>
    public bool AllowsReconnect { get { lock (_gate) return _servers.Exists(s => s.AllowsReconnect); } }

    /// <summary>Gets the number of server tunnels available for new connections.</summary>
    public int ServerCount { get { lock (_gate) return _servers.Count; } }

    /// <summary>Makes a server tunnel available for client attachment.</summary>
    /// <param name="server">Application server tunnel.</param>
    public void Register(RelayPeer server)
    {
        lock (_gate) _servers.Add(server);
        RelayMetrics.Servers.Add(1);
    }

    /// <summary>Withdraws the tunnel, clears its pending calls, and closes its assigned clients.</summary>
    /// <param name="server">Application server tunnel.</param>
    public void Remove(RelayPeer server)
    {
        lock (_gate)
        {
            if (_servers.Remove(server)) RelayMetrics.Servers.Add(-1);
            foreach (var (key, pending) in _invocations)
                if (pending.Caller == server) RemoveInvocation(key, out _);
            foreach (var client in _clients.Values)
                if (client.Server == server) Close(client.Peer.Id);
        }
    }

    /// <summary>Registers a client on an eligible server and forwards the open message.</summary>
    /// <param name="peer">Transport peer for this connection.</param>
    /// <param name="protocol">Negotiated client hub protocol.</param>
    /// <param name="user">User identifier.</param>
    /// <param name="open">Service message announcing the connection.</param>
    /// <param name="identity">Session token and claims for the connection.</param>
    /// <param name="resume">Whether the selected server must support stateful reconnect.</param>
    /// <returns>The attached client, or null if routing or identity constraints prevent attachment.</returns>
    public RelayClient? Attach(RelayPeer peer, string protocol, string? user, OpenConnectionMessage open, RelayIdentity identity, bool resume = false)
    {
        lock (_gate)
        {
            var eligible = resume ? _servers.FindAll(s => s.AllowsReconnect) : _servers;
            if (eligible.Count == 0) return null;
            var preferred = open.Claims.FirstOrDefault(c => c.Type == "asrs.s.sn")?.Value;
            var server = preferred is null ? null : eligible.FirstOrDefault(s => s.ServerId == preferred);
            if (server is null && open.Claims.Any(c => c.Type == "asrs.s.ssticky" && c.Value == "Required")) return null;
            server ??= eligible[RandomUtil.Next(eligible.Count)];
            var client = new RelayClient(peer, server, protocol, user, identity);
            if (_clients.ContainsKey(peer.Id) || (identity.Token.Length != 0 && !_tokens.TryAdd(identity.Token, client))) return null;
            _clients[peer.Id] = client;
            if (user is not null)
            {
                _users.Add(user, client);
                _userGroups.Attach(client, user);
            }
            RelayMetrics.Clients.Add(1);
            server.Send(open);
            return client;
        }
    }

    /// <summary>Removes the client from routing and notifies its server tunnel.</summary>
    /// <param name="client">Attached client whose state is being updated.</param>
    public void Detach(RelayClient client)
    {
        lock (_gate) RemoveClient(client.Peer.Id, abort: false);
    }

    /// <summary>Checks whether a connection identifier belongs to a live client.</summary>
    /// <param name="id">Client connection identifier.</param>
    /// <returns>True if the connection is attached to this hub.</returns>
    public bool Contains(string id) => _clients.ContainsKey(id);

    /// <summary>Delivers a management send to the selected audience and recognizes repeated operation keys.</summary>
    /// <param name="id">Client connection identifier.</param>
    /// <param name="group">Group name.</param>
    /// <param name="user">User identifier.</param>
    /// <param name="excluded">Connection identifiers omitted from delivery.</param>
    /// <param name="json">Framed JSON invocation.</param>
    /// <param name="binary">Framed MessagePack invocation.</param>
    /// <param name="operation">Optional management idempotency key.</param>
    /// <param name="fingerprint">Request hash associated with the operation key.</param>
    /// <returns>The HTTP status code for the send.</returns>
    public int SendManaged(string? id, string? group, string? user, HashSet<string> excluded, byte[] json, ReadOnlyMemory<byte> binary, Guid? operation, string? fingerprint)
    {
        RelayClient[] recipients;
        lock (_gate)
        {
            _userGroups.Expire(DateTimeOffset.UtcNow);
            if (operation is { } key)
            {
                if (_sentOperations.TryGetValue(key, out var previous)) return previous == fingerprint ? 202 : 409;
                if (_sentOperations.Count >= 65536) return 429;
                if (id is not null && !_clients.ContainsKey(id)) return 404;
                _sentOperations.Add(key, fingerprint!);
                _sentExpiry.Enqueue((key, DateTimeOffset.UtcNow.AddMinutes(5)));
            }
            IEnumerable<RelayClient> clients = _clients.Values;
            if (id is not null) clients = _clients.TryGetValue(id, out var client) ? [client] : [];
            else if (group is not null) clients = _groups.Members(group);
            else if (user is not null) clients = _users.Members(user);
            recipients = clients.Where(c => !excluded.Contains(c.Peer.Id)).ToArray();
        }
        foreach (var client in recipients)
            client.Peer.Send(client.Protocol == "json" ? json : binary);
        return 202;
    }

    /// <summary>Checks whether any connections belong to the group.</summary>
    /// <param name="name">Audience name.</param>
    /// <returns>True if the group has a connected member.</returns>
    public bool ContainsGroup(string name)
    {
        lock (_gate) return _groups.Contains(name);
    }

    /// <summary>Checks whether the user has any attached connections.</summary>
    /// <param name="id">User identifier.</param>
    /// <returns>True if the user has a connected client.</returns>
    public bool ContainsUser(string id)
    {
        lock (_gate) return _users.Contains(id);
    }

    /// <summary>Disconnects the client and removes its routing state.</summary>
    /// <param name="id">Client connection identifier.</param>
    /// <returns>True if the client existed.</returns>
    public bool Close(string id)
    {
        lock (_gate) return RemoveClient(id, abort: true);
    }

    /// <summary>Removes membership and pending management waiters and notifies the owning server.</summary>
    /// <param name="id">Client connection identifier.</param>
    /// <param name="abort">Whether to abort the underlying client transport.</param>
    /// <returns>True if the connection was registered.</returns>
    private bool RemoveClient(string id, bool abort)
    {
        if (!_clients.TryRemove(id, out var client)) return false;
        foreach (var waiter in client.ManagementResults.Values) waiter.TrySetException(new IOException("Client disconnected."));
        if (abort) client.Peer.Abort();
        if (client.Identity.Token.Length != 0) _tokens.Remove(client.Identity.Token);
        if (client.UserId is not null) _users.Remove(client.UserId, client);
        foreach (var group in client.Groups.Keys) _groups.Remove(group, client);
        RelayMetrics.Clients.Add(-1);
        client.Server.Send(new CloseConnectionMessage(id));
        return true;
    }

    /// <summary>Routes an incoming service protocol message and sends any required acknowledgement.</summary>
    /// <param name="server">Application server tunnel.</param>
    /// <param name="message">Incoming service protocol message.</param>
    public void Dispatch(RelayPeer server, ServiceMessage message)
    {
        switch (message)
        {
            case ConnectionDataMessage data:
                if (_clients.TryGetValue(data.ConnectionId, out var c)) c.Peer.Send(data.Payload.ToArray());
                break;
            case ClientInvocationMessage invocation:
                lock (_gate)
                {
                    if (!_clients.TryGetValue(invocation.ConnectionId, out var receiver) ||
                        !invocation.Payloads.TryGetValue(receiver.Protocol, out var bytes) || _invocations.Count >= 8192)
                    {
                        server.Send(new ErrorCompletionMessage(invocation.InvocationId, invocation.ConnectionId, invocation.CallerServerId, "Client unavailable."));
                        break;
                    }
                    if (!_invocations.TryAdd((invocation.ConnectionId, invocation.InvocationId),
                        new PendingInvocation(server, invocation.CallerServerId, DateTimeOffset.UtcNow.AddMinutes(2), receiver))) break;
                    receiver.TrackInvocation();
                    if (receiver.Server.ServerId != server.ServerId)
                    {
                        receiver.Server.Send(invocation);
                        server.Send(new ServiceMappingMessage(invocation.InvocationId, invocation.ConnectionId, receiver.Server.Id));
                    }
                    receiver.Peer.Send(bytes.ToArray());
                }
                break;
            case ClientCompletionMessage completion:
                lock (_gate)
                {
                    var key = (completion.ConnectionId, completion.InvocationId);
                    if (!_invocations.TryGetValue(key, out var pending)) break;
                    if (completion.Payload.IsEmpty) RemoveInvocation(key, out _); // Caller SDK has consumed its result.
                    else pending.Caller.Send(completion); // Routed results have no receipt; retain the bounded reply for recovery.
                }
                break;
            case MulticastDataMessage multicast:
                var filter = string.IsNullOrEmpty(multicast.Filter) ? null : SubscriberFilter.Parse(multicast.Filter);
                if (!string.IsNullOrEmpty(multicast.Filter) && filter is null)
                {
                    // A filter is an audience restriction. Discard the whole send if it cannot be evaluated.
                    RelayMetrics.UnsupportedMessages.Add(1);
                    break;
                }
                var encoded = multicast.Payloads.ToDictionary(p => p.Key, p => p.Value.ToArray());
                foreach (var client in Recipients(multicast))
                    if ((filter is null || filter(client)) && encoded.TryGetValue(client.Protocol, out var payload))
                        client.Peer.Send(payload);
                break;
            case UserJoinGroupWithAckMessage join:
                lock (_gate) server.Send(new AckMessage(join.AckId, _userGroups.Join(join.UserId, join.GroupName, join.Ttl) ? 1 : 4));
                break;
            case UserJoinGroupMessage join:
                lock (_gate) _userGroups.Join(join.UserId, join.GroupName, join.Ttl);
                break;
            case UserLeaveGroupWithAckMessage leave:
                lock (_gate) _userGroups.Leave(leave.UserId, leave.GroupName);
                server.Send(new AckMessage(leave.AckId, 1));
                break;
            case UserLeaveGroupMessage leave:
                lock (_gate) _userGroups.Leave(leave.UserId, leave.GroupName);
                break;
            case CheckUserInGroupWithAckMessage check:
                lock (_gate)
                {
                    _userGroups.Expire(DateTimeOffset.UtcNow);
                    bool member = _userGroups.Contains(check.UserId, check.GroupName) || _users.Members(check.UserId).Any(c => c.Groups.ContainsKey(check.GroupName));
                    server.Send(new AckMessage(check.AckId, member ? 1 : 2));
                }
                break;
            case GroupMemberQueryMessage query:
                server.Send(ListMembers(query));
                break;
            case JoinGroupMessage join:
                JoinGroup(join.ConnectionId, join.GroupName);
                break;
            case LeaveGroupMessage leave:
                LeaveGroup(leave.ConnectionId, leave.GroupName);
                break;
            case JoinGroupWithAckMessage join:
                bool joined = JoinGroup(join.ConnectionId, join.GroupName);
                server.Send(new AckMessage(join.AckId, joined ? 1 : 2));
                break;
            case LeaveGroupWithAckMessage leave:
                LeaveGroup(leave.ConnectionId, leave.GroupName);
                server.Send(new AckMessage(leave.AckId, 1));
                break;
            case CloseConnectionMessage close:
                Close(close.ConnectionId);
                break;
#pragma warning disable CS0618 // Older SDK clients still send this wire message.
            case CloseConnectionWithAckMessage close:
                server.Send(new AckMessage(close.AckId, Close(close.ConnectionId) ? 1 : 2));
                break;
            case CloseMultiConnectionsWithAckMessage close:
                lock (_gate)
                {
                    IEnumerable<RelayClient> targets = close switch
                    {
                        CloseUserConnectionsWithAckMessage u => _users.Members(u.UserId),
                        CloseGroupConnectionsWithAckMessage g => _groups.Members(g.GroupName),
                        CloseConnectionsWithAckMessage => _clients.Values,
                        _ => []
                    };
                    var exclusions = close.ExcludedList.ToHashSet(StringComparer.Ordinal);
                    foreach (var target in targets.Where(c => !exclusions.Contains(c.Peer.Id)).ToArray())
                        RemoveClient(target.Peer.Id, abort: true);
                }
                server.Send(new AckMessage(close.AckId, 1));
                break;
#pragma warning restore CS0618
            case CheckConnectionExistenceWithAckMessage check:
                server.Send(new AckMessage(check.AckId, Contains(check.ConnectionId) ? 1 : 2));
                break;
            case GetConnectionClaimsMessage read:
                lock (_gate)
                {
                    var current = ByToken(read.ConnectionToken);
                    server.Send(current?.Identity.Read(read.AckId) ?? new AckMessage(read.AckId, 2));
                }
                break;
            case RefreshAuthMessage refresh:
                lock (_gate)
                {
                    var current = ByToken(refresh.ConnectionToken);
                    server.Send(current?.Identity.Refresh(refresh, current) ?? new AckMessage(refresh.AckId, 2));
                }
                break;
            case CheckGroupExistenceWithAckMessage group:
                lock (_gate) server.Send(new AckMessage(group.AckId, _groups.Contains(group.GroupName) ? 1 : 2));
                break;
            case CheckUserExistenceWithAckMessage user:
                lock (_gate) server.Send(new AckMessage(user.AckId, _users.Contains(user.UserId) ? 1 : 2));
                break;
            case PingMessage ping:
                for (int i = 0; i + 1 < ping.Messages.Length; i += 2)
                {
                    if (ping.Messages[i] != "shutdown") continue;
                    switch (ping.Messages[i + 1])
                    {
                        case "fin:0":
                        case "fin:1":
                            Remove(server);
                            break;
                        case "fin:2":
                            // Stop admitting clients; existing calls and result waiters retain their tunnel until it closes.
                            lock (_gate)
                                if (_servers.Remove(server)) RelayMetrics.Servers.Add(-1);
                            break;
                        default: continue;
                    }
                    server.Send(new PingMessage { Messages = ["shutdown", "finack"] });
                }
                if (ping.Messages.Contains("echo")) server.Send(ping);
                if (ping.Messages.Contains("status")) server.Send(new PingMessage { Messages = ["status", _clients.IsEmpty ? "0" : "1"] });
                break;
            case IAckableMessage ack:
                server.Send(new AckMessage(ack.AckId, 4, "Unsupported relay operation."));
                break;
            default:
                RelayMetrics.UnsupportedMessages.Add(1);
                break;
        }
    }

    /// <summary>Invokes a method on one client and shares its completion across matching retry requests.</summary>
    /// <param name="http">HTTP context for the current request.</param>
    /// <param name="connectionId">Target client connection identifier.</param>
    /// <param name="invocationId">Identifier used to correlate the client's completion.</param>
    /// <param name="json">Framed JSON invocation.</param>
    /// <param name="binary">Framed MessagePack invocation.</param>
    /// <param name="operation">Optional management idempotency key.</param>
    /// <param name="fingerprint">Request hash associated with the operation key.</param>
    /// <returns>The client's framed JSON completion or an HTTP error result.</returns>
    public async Task<IResult> InvokeManaged(HttpContext http, string connectionId, string invocationId, byte[] json,
        ReadOnlyMemory<byte> binary, Guid? operation, string? fingerprint)
    {
        Task<byte[]> result;
        lock (_gate)
        {
            if (!_clients.TryGetValue(connectionId, out var client)) return Results.NotFound();
            var now = DateTimeOffset.UtcNow;
            if (operation is { } key && client.RetryableResults.TryGetValue(key, out var prior) && prior.Expires > now)
            {
                if (prior.Fingerprint != fingerprint) return Results.Conflict();
                result = prior.Reply;
            }
            else
            {
                foreach (var (id, entry) in client.RetryableResults)
                    if (entry.Expires <= now) client.RetryableResults.Remove(id);
                if (client.ManagementResults.Count >= 64 || (operation.HasValue && client.RetryableResults.Count >= 64)) return Results.StatusCode(429);
                var waiter = new TaskCompletionSource<byte[]>(TaskCreationOptions.RunContinuationsAsynchronously);
                client.ManagementResults.Add(invocationId, waiter);
                client.TrackInvocation();
                result = WaitForCompletion(client, invocationId, waiter.Task, operation.HasValue);
                if (operation.HasValue)
                    client.RetryableResults.Add(operation.Value, new CachedClientResult(fingerprint!, now.AddMinutes(5), result));
                client.Peer.Send(client.Protocol == "json" ? json : binary);
            }
        }
        try
        {
            var frame = await result.WaitAsync(http.RequestAborted);
            http.Response.Headers["X-ASRS-Protocol"] = "json";
            return Results.Bytes(frame, "application/octet-stream");
        }
        catch (TimeoutException) { return Results.StatusCode(504); }
        catch (InvalidDataException) { return Results.StatusCode(413); }
        catch (IOException) { return Results.StatusCode(410); }
    }

    /// <summary>Waits for a bounded client reply and releases its invocation slot on every exit.</summary>
    /// <param name="client">Attached client whose state is being updated.</param>
    /// <param name="id">Client invocation identifier.</param>
    /// <param name="completion">Pending task for the client's reply.</param>
    /// <param name="retryable">Whether the reply must fit the retention limit for retries.</param>
    /// <returns>The complete JSON response frame.</returns>
    private async Task<byte[]> WaitForCompletion(RelayClient client, string id, Task<byte[]> completion, bool retryable)
    {
        try
        {
            var bytes = await completion.WaitAsync(TimeSpan.FromSeconds(30));
            if (retryable && bytes.Length > 16 * 1024) throw new InvalidDataException("Retryable result exceeds the retention limit.");
            return bytes;
        }
        finally
        {
            lock (_gate)
            {
                client.ManagementResults.Remove(id);
                client.ReleaseInvocation();
            }
        }
    }

    /// <summary>Consumes management completion identifiers and resolves a matching waiter when present.</summary>
    /// <param name="client">Attached client whose state is being updated.</param>
    /// <param name="id">Client invocation identifier.</param>
    /// <param name="frame">Complete client frame including framing bytes.</param>
    /// <param name="body">Hub message body without framing bytes.</param>
    /// <returns>True for a management invocation identifier, including a late completion.</returns>
    public bool CompleteManaged(RelayClient client, string id, ReadOnlySequence<byte> frame, ReadOnlySequence<byte> body)
    {
        if (!id.StartsWith("cf:", StringComparison.Ordinal)) return false;
        lock (_gate)
        {
            if (client.ManagementResults.TryGetValue(id, out var waiter))
                waiter.TrySetResult(client.Protocol == "json" ? frame.ToArray() : ClientFrame.JsonCompletion(body, id));
        }
        return true;
    }

    /// <summary>Retains the first client completion for bounded retransmission to the calling server.</summary>
    /// <param name="client">Attached client whose state is being updated.</param>
    /// <param name="id">Client invocation identifier.</param>
    /// <param name="frame">Complete client frame including framing bytes.</param>
    public void RememberCompletion(RelayClient client, string id, ReadOnlySequence<byte> frame)
    {
        lock (_gate)
        {
            if (!_invocations.TryGetValue((client.Peer.Id, id), out var pending) || pending.Reply is not null) return;
            if (_replyBytes + frame.Length > 8 * 1024 * 1024)
            {
                RemoveInvocation((client.Peer.Id, id), out _);
                pending.Caller.Send(new ErrorCompletionMessage(id, client.Peer.Id, pending.CallerId, "Result buffer at capacity."));
                return;
            }
            pending.Reply = frame.ToArray();
            _replyBytes += pending.Reply.Length;
            pending.RetryAfter = DateTimeOffset.UtcNow.AddMilliseconds(250);
        }
    }

    /// <summary>Expires grants and operation keys, retires stale calls, and retries retained completions.</summary>
    /// <param name="now">Time used to evaluate expirations and retries.</param>
    public void Maintain(DateTimeOffset now)
    {
        lock (_gate)
        {
            _userGroups.Expire(now);
            while (_sentExpiry.TryPeek(out var op) && op.Expires <= now) _sentOperations.Remove(_sentExpiry.Dequeue().Id);
            foreach (var (key, pending) in _invocations)
            {
                if (pending.Deadline <= now || !_clients.ContainsKey(key.ClientId))
                {
                    RemoveInvocation(key, out _);
                    pending.Caller.Send(new ErrorCompletionMessage(key.InvocationId, key.ClientId, pending.CallerId, "Invocation ended."));
                }
                else if (pending.Reply is { } reply && pending.RetryAfter <= now)
                {
                    // SDK 1.34 can receive a result before its caller registers the invocation. Send only
                    // the captured completion back to the caller. Never execute the client method again.
                    RelayMetrics.RecoveredReplies.Add(1);
                    pending.Caller.Send(new ClientCompletionMessage(key.InvocationId, key.ClientId, pending.CallerId, pending.Receiver.Protocol, reply));
                    // The stock SDK has no receipt for routed completions. Bound both retries and retention.
                    // Repeating the result cannot execute the client method or complete its task twice.
                    pending.RecoveryAttempts++;
                    if (pending.RecoveryAttempts >= 3) RemoveInvocation(key, out _);
                    else pending.RetryAfter = now.AddSeconds(pending.RecoveryAttempts == 1 ? 1 : 4);
                }
            }
        }
    }

    /// <summary>Releases a pending invocation and its retained reply bytes.</summary>
    /// <param name="key">Client and invocation identifiers for the pending call.</param>
    /// <param name="pending">Removed invocation, or null if the key was absent.</param>
    /// <returns>True if a pending invocation was removed.</returns>
    private bool RemoveInvocation((string ClientId, string InvocationId) key, out PendingInvocation? pending)
    {
        if (!_invocations.Remove(key, out pending)) return false;
        pending.Receiver.ReleaseInvocation();
        _replyBytes -= pending.Reply?.Length ?? 0;
        return true;
    }

    /// <summary>Grants explicit group membership to a connected client.</summary>
    /// <param name="connectionId">Target client connection identifier.</param>
    /// <param name="group">Group name.</param>
    /// <returns>True if the client exists.</returns>
    public bool JoinGroup(string connectionId, string group)
    {
        lock (_gate)
        {
            if (!_clients.TryGetValue(connectionId, out var client)) return false;
            _groups.Add(group, client);
            client.Groups[group] = 0;
            return true;
        }
    }

    /// <summary>Removes the connection from one group or, when the group is null, from all groups.</summary>
    /// <param name="connectionId">Target client connection identifier.</param>
    /// <param name="group">Group name.</param>
    public void LeaveGroup(string connectionId, string? group)
    {
        lock (_gate)
        {
            if (!_clients.TryGetValue(connectionId, out var client)) return;
            if (group is null)
            {
                foreach (var name in client.Groups.Keys) _groups.Remove(name, client);
                client.Groups.Clear();
            }
            else
            {
                _groups.Remove(group, client);
                client.Groups.TryRemove(group, out _);
            }
        }
    }

    /// <summary>Encodes a page of group members and its continuation token in a service acknowledgement.</summary>
    /// <param name="query">Group membership query and paging limits.</param>
    /// <returns>An acknowledgement containing the requested page, or an error for invalid paging limits.</returns>
    private AckMessage ListMembers(GroupMemberQueryMessage query)
    {
        if (query.Top is <= 0 || query.MaxPageSize is <= 0 or > 200) return new AckMessage(query.AckId, 4);
        var limit = Math.Min(query.MaxPageSize ?? 100, query.Top ?? int.MaxValue);
        lock (_gate)
        {
            _userGroups.Expire(DateTimeOffset.UtcNow);
            var page = _groups.Members(query.GroupName).Where(c => string.CompareOrdinal(c.Peer.Id, query.ContinuationToken) > 0)
                .OrderBy(c => c.Peer.Id, StringComparer.Ordinal).Take(limit + 1).ToArray();
            using var writer = new MessagePack.MessagePackWriter();
            writer.WriteArrayHeader(2);
            writer.WriteArrayHeader(Math.Min(page.Length, limit));
            foreach (var member in page.Take(limit))
            {
                writer.WriteArrayHeader(2);
                writer.WriteString(member.Peer.Id);
                writer.WriteString(member.UserId);
            }
            writer.WriteString(page.Length > limit && query.Top != limit ? page[limit - 1].Peer.Id : null);
            return new AckMessage(query.AckId, 1) { Payload = new ReadOnlySequence<byte>(writer.ToArray()) };
        }
    }

    /// <summary>Looks up an attached client by session token.</summary>
    /// <param name="token">Session token.</param>
    /// <returns>The attached client, or null if the token is unknown.</returns>
    private RelayClient? ByToken(string token) => _tokens.GetValueOrDefault(token);

    /// <summary>Takes a snapshot of the connections selected by a multicast message and its exclusions.</summary>
    /// <param name="message">Incoming service protocol message.</param>
    /// <returns>Matching clients at the time of the lookup.</returns>
    private RelayClient[] Recipients(MulticastDataMessage message)
    {
        lock (_gate)
        {
            _userGroups.Expire(DateTimeOffset.UtcNow);
            IEnumerable<RelayClient> clients = message switch
            {
                BroadcastDataMessage b => _clients.Values.Where(c => !b.ExcludedList.Contains(c.Peer.Id)),
                GroupBroadcastDataMessage g => _groups.Members(g.GroupName).Where(c =>
                    !g.ExcludedList.Contains(c.Peer.Id) && (c.UserId is null || !g.ExcludedUserList.Contains(c.UserId))),
                MultiGroupBroadcastDataMessage g => g.GroupList.SelectMany(_groups.Members).Distinct(),
                UserDataMessage u => _users.Members(u.UserId),
                MultiUserDataMessage u => u.UserList.Distinct().SelectMany(_users.Members),
                MultiConnectionDataMessage m => m.ConnectionList.Distinct().Select(id => _clients.GetValueOrDefault(id)).OfType<RelayClient>(),
                _ => []
            };
            return clients.ToArray();
        }
    }
}
