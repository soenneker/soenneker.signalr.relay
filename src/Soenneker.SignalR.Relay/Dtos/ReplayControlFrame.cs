namespace Soenneker.SignalR.Relay.Dtos;

/// <summary>A JSON acknowledgement or sequence marker for stateful reconnect.</summary>
/// <param name="Type">Hub control message type.</param>
/// <param name="SequenceId">Frame sequence number.</param>
internal sealed record ReplayControlFrame(int Type, long SequenceId);
