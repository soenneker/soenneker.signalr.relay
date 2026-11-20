using Microsoft.AspNetCore.SignalR;
using System.Threading.Tasks;

namespace Soenneker.SignalR.Relay.Tests;

public sealed class RoutingHub : Hub
{
    public Task Join(string group) => Groups.AddToGroupAsync(Context.ConnectionId, group);
    public Task Leave(string group) => Groups.RemoveFromGroupAsync(Context.ConnectionId, group);
    public Task Subscribe() => Groups.AddToGroupAsync(Context.ConnectionId, "subscribers");
}
