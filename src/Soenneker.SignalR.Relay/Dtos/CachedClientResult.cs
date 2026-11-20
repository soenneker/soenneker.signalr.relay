namespace Soenneker.SignalR.Relay.Dtos;

/// <summary>Associates a management invocation with the completion shared by retries.</summary>
/// <param name="Fingerprint">Hash of the original request.</param>
/// <param name="Expires">Time after which the operation key can be reused.</param>
/// <param name="Reply">Task producing the framed JSON completion.</param>
internal sealed record CachedClientResult(string Fingerprint, DateTimeOffset Expires, Task<byte[]> Reply);
