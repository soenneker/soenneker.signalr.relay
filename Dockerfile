FROM mcr.microsoft.com/dotnet/sdk:11.0-resolute-aot AS build
WORKDIR /source
COPY src/Soenneker.SignalR.Relay/ ./relay/
RUN dotnet publish relay/Soenneker.SignalR.Relay.csproj -c Release -o /app && rm -f /app/*.dbg
FROM mcr.microsoft.com/dotnet/runtime-deps:11.0-resolute
WORKDIR /app
COPY --from=build /app .
ENV ASPNETCORE_HTTP_PORTS=8080
EXPOSE 8080
USER $APP_UID
ENTRYPOINT ["./Soenneker.SignalR.Relay"]
