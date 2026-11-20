namespace Soenneker.Cloudflare.SignalR;

internal sealed class RelayMaintenance(IRelayDirectory directory) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        using var timer = new PeriodicTimer(TimeSpan.FromMilliseconds(250));
        while (await timer.WaitForNextTickAsync(stoppingToken))
            directory.Maintain(DateTimeOffset.UtcNow);
    }
}
