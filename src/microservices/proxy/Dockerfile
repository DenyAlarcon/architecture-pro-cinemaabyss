# Build stage
FROM mcr.microsoft.com/dotnet/sdk:8.0 AS build
WORKDIR /src
COPY ProxyService.csproj ./
RUN dotnet restore "ProxyService.csproj"
COPY . ./
RUN dotnet publish "ProxyService.csproj" -c Release -o /app/publish

# Runtime stage
FROM mcr.microsoft.com/dotnet/aspnet:8.0 AS runtime
WORKDIR /app
COPY --from=build /app/publish ./
ENV ASPNETCORE_URLS=http://0.0.0.0:8000
EXPOSE 8000
ENTRYPOINT ["dotnet", "ProxyService.dll"]
