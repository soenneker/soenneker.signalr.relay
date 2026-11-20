using Microsoft.AspNetCore.Cors.Infrastructure;
using Microsoft.Extensions.Options;

namespace Soenneker.SignalR.Relay;

/// <summary>Configures the relay's default CORS policy from its allowed origins.</summary>
internal sealed class RelayCorsOptions : IConfigureOptions<CorsOptions>
{
    /// <summary>Browser origins permitted to make credentialed requests.</summary>
    private readonly string[] _origins;

    /// <summary>Reads allowed origins from the relay host configuration.</summary>
    /// <param name="configuration">Configuration supplied by the relay host.</param>
    public RelayCorsOptions(IConfiguration configuration)
    {
        _origins = configuration.GetSection("Relay:AllowedOrigins").Get<string[]>() ?? [];
    }

    public void Configure(CorsOptions options) => options.AddDefaultPolicy(policy =>
    {
        if (_origins.Length > 0) policy.WithOrigins(_origins).AllowAnyHeader().AllowAnyMethod().AllowCredentials();
    });
}
