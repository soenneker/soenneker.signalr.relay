using System.Diagnostics.Metrics;

namespace Soenneker.Cloudflare.SignalR;

internal static class RelayMetrics
{
    private static readonly Meter Meter = new("Soenneker.Cloudflare.SignalR");
    internal static readonly UpDownCounter<long> Clients = Meter.CreateUpDownCounter<long>("relay.clients.active");
    internal static readonly UpDownCounter<long> Servers = Meter.CreateUpDownCounter<long>("relay.server_tunnels.active");
    internal static readonly Counter<long> OutputOverflows = Meter.CreateCounter<long>("relay.output.overflows");
    internal static readonly UpDownCounter<long> QueuedBytes = Meter.CreateUpDownCounter<long>("relay.output.queued_bytes", "By");
    internal static readonly Counter<long> ShedClients = Meter.CreateCounter<long>("relay.output.clients_shed");
    internal static readonly Counter<long> RecoveredReplies = Meter.CreateCounter<long>("relay.invocation_replies.retried");
    internal static readonly Counter<long> UnsupportedMessages = Meter.CreateCounter<long>("relay.service_messages.unsupported");
}
