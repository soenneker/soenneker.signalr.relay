using System.Diagnostics.Metrics;

namespace Soenneker.SignalR.Relay;

/// <summary>Publishes relay connection, queue, and invocation measurements.</summary>
internal static class RelayMetrics
{
    /// <summary>Meter shared by relay instruments.</summary>
    private static readonly Meter Meter = new("Soenneker.SignalR.Relay");

    /// <summary>Current number of attached clients.</summary>
    internal static readonly UpDownCounter<long> Clients = Meter.CreateUpDownCounter<long>("relay.clients.active");

    /// <summary>Current number of available server tunnels.</summary>
    internal static readonly UpDownCounter<long> Servers = Meter.CreateUpDownCounter<long>("relay.server_tunnels.active");

    /// <summary>Number of output queue admissions rejected for capacity.</summary>
    internal static readonly Counter<long> OutputOverflows = Meter.CreateCounter<long>("relay.output.overflows");

    /// <summary>Bytes currently charged to relay output queues.</summary>
    internal static readonly UpDownCounter<long> QueuedBytes = Meter.CreateUpDownCounter<long>("relay.output.queued_bytes", "By");

    /// <summary>Clients whose queues were evicted to free output capacity.</summary>
    internal static readonly Counter<long> ShedClients = Meter.CreateCounter<long>("relay.output.clients_shed");

    /// <summary>Captured invocation replies retransmitted during recovery.</summary>
    internal static readonly Counter<long> RecoveredReplies = Meter.CreateCounter<long>("relay.invocation_replies.retried");

    /// <summary>Service messages discarded because their operation or filter is unsupported.</summary>
    internal static readonly Counter<long> UnsupportedMessages = Meter.CreateCounter<long>("relay.service_messages.unsupported");
}
