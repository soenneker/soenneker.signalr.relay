namespace Soenneker.SignalR.Relay.Dtos;

/// <summary>The route and sorted exclusions included in a management request fingerprint.</summary>
/// <param name="Path">Requested management route.</param>
/// <param name="Excluded">Excluded connection identifiers in ordinal order.</param>
internal sealed record ManagementAudience(string? Path, string[] Excluded);
