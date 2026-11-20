FROM mcr.microsoft.com/dotnet/sdk:11.0 AS build
WORKDIR /source
COPY src/Soenneker.Cloudflare.SignalR/ ./relay/
RUN dotnet publish relay/Soenneker.Cloudflare.SignalR.csproj -c Release -o /app --no-self-contained
FROM mcr.microsoft.com/dotnet/aspnet:11.0
WORKDIR /app
COPY --from=build /app .
ENV ASPNETCORE_HTTP_PORTS=8080
EXPOSE 8080
USER $APP_UID
ENTRYPOINT ["dotnet", "Soenneker.Cloudflare.SignalR.dll"]
