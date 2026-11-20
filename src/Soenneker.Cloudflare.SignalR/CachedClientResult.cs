namespace Soenneker.Cloudflare.SignalR;

internal sealed record CachedClientResult(string Fingerprint, DateTimeOffset Expires, Task<byte[]> Reply);
