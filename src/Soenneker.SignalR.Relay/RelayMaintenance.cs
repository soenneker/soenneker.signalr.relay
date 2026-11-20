using Soenneker.SignalR.Relay.Abstract;

namespace Soenneker.SignalR.Relay;

/// <summary>Schedules periodic cleanup and invocation recovery across all relay hubs.</summary>
/// <param name="directory">Hub and session directory for this relay.</param>
internal sealed class RelayMaintenance(IRelayDirectory directory) : BackgroundService
{
    /// <summary>Runs hub maintenance every quarter second until host shutdown.</summary>
    /// <param name="stoppingToken">Signals host shutdown.</param>
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        using var timer = new PeriodicTimer(TimeSpan.FromMilliseconds(250));
        while (await timer.WaitForNextTickAsync(stoppingToken))
            directory.Maintain(DateTimeOffset.UtcNow);
    }
}
