# Soenneker.Cloudflare.SignalR

A .NET 11 relay for ASP.NET Core SignalR. The application hosts the hub and authorizes connections. The relay holds client transports and multiplexes them over outbound application-server WebSockets using Microsoft's service protocol.

## Container

Build the Linux image from the repository root.

```sh
docker build --platform linux/amd64 -t soenneker/cloudflare-signalr:local .
```

The image listens on port 8080. Configure these environment variables in the container.

| Variable | Value |
| --- | --- |
| `Relay__Endpoint` | Public HTTPS origin of this relay. Loopback HTTP is supported for development. |
| `Relay__AccessKey` | Shared signing secret, at least 32 bytes. Generate a separate random key for each environment. |
| `Relay__MaxQueuedBytes` | Shared outbound queue budget in bytes. Defaults to 67108864 (64 MiB), including writes waiting on transports. One eighth is reserved for server tunnel control traffic. |
| `Relay__DrainSeconds` | Maximum time for existing clients to finish work on planned shutdown. Defaults to 5 seconds. |
| `Relay__PreviousAccessKey` | Optional previous key during rotation. Remove it after the migration window. |
| `Relay__AllowedOrigins__0` | Exact browser application origin. Additional origins use consecutive indexes. |

A Worker forwards negotiation, client transports, management requests and API tunnels to one named container per endpoint. For redundancy, deploy a second Worker and container with a distinct origin and signing key. Register both endpoints on every API host. Connections and groups live in memory. Clients that enable stateful reconnect can resume a briefly interrupted WebSocket on the same running relay, preserving membership. After relay or API process loss, clients renegotiate, rebuild membership and refresh missed data. Replay holds up to 1 MiB and 1,024 unacknowledged messages per connection, with a shared 64 MiB process limit. An exhausted buffer ends the session.

## Operations

`GET /health` checks the process. `GET /ready/apphub` returns 200 when the named hub has a client-capable API tunnel and 503 during cold start or shutdown. The Leadping deploy script accepts `--replica` to generate a second Worker, custom domain, container and secret binding. Both endpoints incur usage charges.

The `Soenneker.Cloudflare.SignalR` .NET meter reports connected clients and server tunnels, output queue overflows, unsupported messages and retried invocation replies. A collector can subscribe to this meter. Client frames have a 1 MiB limit and each peer has a 4 MiB output budget. Client input shares at most 3 MiB of the API tunnel's queue, leaving capacity for control traffic.

On Linux SIGTERM the relay marks hubs unready, rejects new client and server connections with HTTP 503, and continues serving admitted sessions until clients leave or the drain window expires. Under queue pressure the relay sheds backlogged clients. The meter reports queued bytes in `relay.output.queued_bytes` and pressure evictions in `relay.output.clients_shed`.

For key rotation, deploy the new current key and the previous key and restart the container so it loads the environment. Roll the API connection strings, allow old tokens to expire, then remove the previous key and restart. In Leadping, `--with-previous-key` binds the secret named after the current secret with the suffix `__PREVIOUS`. Roll endpoints one at a time and check hub readiness before advancing.

## ASP.NET Core

Register the `Soenneker.Cloudflare.SignalR.AspNetCore` package on each application server. Supply a service connection string of the form `Endpoint=https://relay.example.com;AccessKey=<secret>;Version=1.0;`. Existing browser and .NET clients continue connecting to the application's mapped hub.

## Compatibility

The relay targets the ASP.NET Core application-server mode of Microsoft.Azure.SignalR 1.34.0. The integration tests exercise cross-server group sends, hub invocations, client results, permission changes, authentication refresh and immediate session revocation. The management HTTP surface supports health, connection and audience checks, connection group membership, JSON sends and client invocations. Keyed management sends retain a receipt for five minutes per live relay. Keyed client invocations retain up to 64 results per connection for five minutes, each at most 16 KiB. Multi-region failover, durable message replay, the full Azure management API and serverless Functions bindings are outside this implementation. Production capacity and outage recovery depend on the deployment.
