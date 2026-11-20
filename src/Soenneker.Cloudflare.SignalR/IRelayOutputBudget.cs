namespace Soenneker.Cloudflare.SignalR;

internal interface IRelayOutputBudget
{
    object Gate { get; }
    bool TryReserve(RelayPeer peer, int bytes, bool serverControl, out List<RelayPeer>? evicted);
    void Release(RelayPeer peer, int bytes);
}
