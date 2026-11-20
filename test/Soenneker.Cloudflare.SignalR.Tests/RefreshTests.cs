using System;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Net.Http.Json;
using System.Net.Sockets;
using System.Net.WebSockets;
using System.Security.Claims;
using System.Security.Cryptography;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Authentication.JwtBearer;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.IdentityModel.JsonWebTokens;
using Microsoft.IdentityModel.Tokens;

namespace Soenneker.Cloudflare.SignalR.Tests;

public sealed class RefreshTests
{
    [Test]
    public async Task RefreshPreservesIdentityAndRejectsPermissionChange()
    {
        using var deadline = new CancellationTokenSource(TimeSpan.FromSeconds(20));
        var token = deadline.Token;
        using var port = new TcpListener(IPAddress.Loopback, 0);
        port.Start();
        var endpoint = $"http://127.0.0.1:{((IPEndPoint)port.LocalEndpoint).Port}";
        port.Stop();
        string relayKey = Convert.ToBase64String(RandomNumberGenerator.GetBytes(48));
        var appKey = new SymmetricSecurityKey(RandomNumberGenerator.GetBytes(48));
        await using var relay = RelayApplication.Build([$"--Relay:Endpoint={endpoint}", $"--Relay:AccessKey={relayKey}", $"--urls={endpoint}"]);
        await relay.StartAsync(token);
        var builder = WebApplication.CreateBuilder();
        builder.WebHost.UseUrls("http://127.0.0.1:0");
        builder.Services.AddAuthentication(JwtBearerDefaults.AuthenticationScheme).AddJwtBearer(o =>
            o.TokenValidationParameters = new TokenValidationParameters { ValidIssuer = "test", ValidAudience = "api", IssuerSigningKey = appKey });
        builder.Services.AddAuthorization();
        builder.Services.AddSignalR().AddAzureSignalR(o => { o.ConnectionString = $"Endpoint={endpoint};AccessKey={relayKey};"; o.InitialHubServerConnectionCount = 1; });
        await using var api = builder.Build();
        api.MapHub<RoutingHub>("/app", o => { o.CloseOnAuthenticationExpiration = true; o.EnableAuthenticationRefresh = true; }).RequireAuthorization();
        await api.StartAsync(token);
        try
        {
            while (relay.Services.GetRequiredService<IRelayDirectory>().Get("routinghub").ServerCount == 0) await Task.Delay(20, token);
            using var app = new HttpClient { BaseAddress = new Uri(api.Urls.Single()) };
            app.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Bearer", Token("reader", appKey));
            using var redirect = await app.PostAsync("/app/negotiate?negotiateVersion=1", null, token);
            redirect.EnsureSuccessStatusCode();
            var negotiation = await redirect.Content.ReadFromJsonAsync<JsonElement>(token);
            var address = negotiation.GetProperty("url").GetString()!;
            var bearer = negotiation.GetProperty("accessToken").GetString()!;
            using var service = new HttpClient();
            service.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Bearer", bearer);
            var uri = new UriBuilder(address);
            uri.Path = "/client/negotiate";
            if (!uri.Query.Contains("negotiateVersion=")) uri.Query = uri.Query.TrimStart('?') + "&negotiateVersion=1";
            using var wrongHub = await service.PostAsync(endpoint + "/client/negotiate?hub=other", null, token);
            await Assert.That(wrongHub.StatusCode).IsEqualTo(HttpStatusCode.Unauthorized);
            using var wrongRole = await service.PostAsync(endpoint + "/server/negotiate?hub=routinghub", null, token);
            await Assert.That(wrongRole.StatusCode).IsEqualTo(HttpStatusCode.Unauthorized);
            using var clientNegotiation = await service.PostAsync(uri.Uri, null, token);
            clientNegotiation.EnsureSuccessStatusCode();
            var session = await clientNegotiation.Content.ReadFromJsonAsync<JsonElement>(token);
            var connectionToken = session.GetProperty("connectionToken").GetString()!;
            using var socket = new ClientWebSocket();
            socket.Options.SetRequestHeader("Authorization", "Bearer " + bearer);
            uri = new UriBuilder(address) { Scheme = "ws" };
            uri.Query = uri.Query.TrimStart('?') + "&id=" + Uri.EscapeDataString(connectionToken);
            await socket.ConnectAsync(uri.Uri, token);
            await socket.SendAsync("{\"protocol\":\"json\",\"version\":1}\u001e"u8.ToArray(), WebSocketMessageType.Text, true, token);
            await socket.ReceiveAsync(new byte[4096], token);
            app.DefaultRequestHeaders.Add("X-SignalR-Connection-Token", connectionToken);
            using var refresh = await app.PostAsync("/app/refresh", null, token);
            refresh.EnsureSuccessStatusCode();
            app.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Bearer", Token("admin", appKey));
            using var denied = await app.PostAsync("/app/refresh", null, token);
            await Assert.That(denied.StatusCode).IsEqualTo(HttpStatusCode.Forbidden);
        }
        finally { await api.StopAsync(); await relay.StopAsync(); }
    }

    private static string Token(string permission, SecurityKey key) => new JsonWebTokenHandler().CreateToken(new SecurityTokenDescriptor
    {
        Issuer = "test", Audience = "api", Expires = DateTime.UtcNow.AddMinutes(5),
        SigningCredentials = new SigningCredentials(key, SecurityAlgorithms.HmacSha256),
        Subject = new ClaimsIdentity([new Claim("sub", "employee"), new Claim("permission", permission)])
    });
}
