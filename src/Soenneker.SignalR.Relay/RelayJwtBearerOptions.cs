using System.Text;
using Microsoft.AspNetCore.Authentication.JwtBearer;
using Microsoft.Extensions.Options;
using Microsoft.IdentityModel.Tokens;

namespace Soenneker.SignalR.Relay;

/// <summary>Configures bearer authentication for relay transports and management requests.</summary>
internal sealed class RelayJwtBearerOptions : IConfigureNamedOptions<JwtBearerOptions>
{
    /// <summary>Origin used in the service protocol's token audiences.</summary>
    private readonly string _audienceOrigin;

    /// <summary>Current and optional previous keys accepted during key rotation.</summary>
    private readonly List<SecurityKey> _signingKeys;

    /// <summary>Reads and validates the relay endpoint and signing keys at startup.</summary>
    /// <param name="configuration">Configuration supplied by the relay host.</param>
    public RelayJwtBearerOptions(IConfiguration configuration)
    {
        var endpoint = new Uri(configuration["Relay:Endpoint"] ?? throw new InvalidOperationException("Relay:Endpoint is required."));
        if ((!endpoint.IsLoopback && endpoint.Scheme != "https") || endpoint.AbsolutePath != "/" || endpoint.Query.Length != 0 || endpoint.Fragment.Length != 0)
            throw new InvalidOperationException("Relay:Endpoint must be an HTTPS origin.");
        var key = Encoding.UTF8.GetBytes(configuration["Relay:AccessKey"] ?? "");
        if (key.Length < 32) throw new InvalidOperationException("Relay:AccessKey must contain at least 32 bytes.");
        _audienceOrigin = $"{endpoint.Scheme}://{endpoint.Host}";
        _signingKeys = [new SymmetricSecurityKey(key)];
        if (configuration["Relay:PreviousAccessKey"] is { Length: > 0 } previous)
        {
            var previousBytes = Encoding.UTF8.GetBytes(previous);
            if (previousBytes.Length < 32) throw new InvalidOperationException("Relay:PreviousAccessKey must contain at least 32 bytes.");
            _signingKeys.Add(new SymmetricSecurityKey(previousBytes));
        }
    }

    public void Configure(string? name, JwtBearerOptions options)
    {
        if (name != JwtBearerDefaults.AuthenticationScheme) return;
        options.MapInboundClaims = true;
        options.TokenValidationParameters = new TokenValidationParameters
        {
            ValidateIssuer = true,
            ValidIssuer = "azure-signalr",
            ValidateAudience = true,
            ValidateLifetime = true,
            ClockSkew = TimeSpan.Zero,
            IssuerSigningKeys = _signingKeys,
            ValidateIssuerSigningKey = true,
            ValidAlgorithms = [SecurityAlgorithms.HmacSha256, SecurityAlgorithms.HmacSha512],
            AudienceValidator = (audiences, _, _) => audiences.Any(a => a.StartsWith($"{_audienceOrigin}/", StringComparison.Ordinal))
        };
        options.Events = new JwtBearerEvents
        {
            OnMessageReceived = context =>
            {
                if (context.Request.Path.StartsWithSegments("/client") && HttpMethods.IsGet(context.Request.Method))
                    context.Token = context.Request.Query["access_token"];
                return Task.CompletedTask;
            },
            OnTokenValidated = context =>
            {
                var request = context.Request;
                if (request.Path.StartsWithSegments("/api/hubs"))
                {
                    if (!context.Principal!.HasClaim("aud", $"{_audienceOrigin}{request.Path.ToUriComponent()}")) context.Fail("Invalid management audience.");
                    return Task.CompletedTask;
                }
                var hub = request.Query["hub"];
                var path = request.Path.StartsWithSegments("/server") ? "server" : "client";
                if (hub.Count != 1 || string.IsNullOrWhiteSpace(hub[0]) || hub[0]!.Length > 200 ||
                    !context.Principal!.HasClaim("aud", $"{_audienceOrigin}/{path}/?hub={hub[0]}"))
                    context.Fail("Invalid relay audience.");
                return Task.CompletedTask;
            }
        };
    }

    public void Configure(JwtBearerOptions options) => Configure(Options.DefaultName, options);
}
