namespace Soenneker.SignalR.Relay.Abstract;

/// <summary>Tracks relay hubs and the session tokens bound to them.</summary>
internal interface IRelayDirectory
{
    /// <summary>Binds a session token to a hub and identity.</summary>
    /// <param name="hub">The hub name.</param>
    /// <param name="identity">The identity whose token is being bound.</param>
    /// <returns>True if the binding was added or the token is empty. False if the token is already bound.</returns>
    bool TryBind(string hub, RelayIdentity identity);

    /// <summary>Checks whether a session token has an active binding.</summary>
    /// <param name="token">The session token to look up.</param>
    /// <returns>True if the token is bound.</returns>
    bool IsBound(string token);

    /// <summary>Checks whether a session token can be used to resume a connection.</summary>
    /// <param name="hub">The requested hub name.</param>
    /// <param name="token">The session token presented by the client.</param>
    /// <param name="principal">The principal requesting the connection.</param>
    /// <returns>True if the token is unbound or its binding matches the hub and the principal’s authorization claims.</returns>
    bool CanResume(string hub, string token, System.Security.Claims.ClaimsPrincipal principal);

    /// <summary>Removes the binding for an identity’s session token.</summary>
    /// <param name="identity">The identity whose token is being released.</param>
    void Unbind(RelayIdentity identity);

    /// <summary>Expires stale state across all registered hubs.</summary>
    /// <param name="now">The time against which to evaluate expirations.</param>
    void Maintain(DateTimeOffset now);

    /// <summary>Looks up a hub by name.</summary>
    /// <param name="name">The hub name.</param>
    /// <returns>The registered hub, or <see langword="null"/> if none exists.</returns>
    RelayHub? Find(string name);

    /// <summary>Gets or creates a hub by name.</summary>
    /// <param name="name">The hub name.</param>
    /// <returns>The registered hub for that name.</returns>
    RelayHub Get(string name);
}
