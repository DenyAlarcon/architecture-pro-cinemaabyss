# Build stage
FROM mcr.microsoft.com/dotnet/sdk:8.0 AS build
WORKDIR /src
COPY EventsService.csproj ./
RUN dotnet restore "EventsService.csproj"
COPY . ./
RUN dotnet publish "EventsService.csproj" -c Release -o /app/publish

# Runtime stage
FROM mcr.microsoft.com/dotnet/aspnet:8.0 AS runtime
WORKDIR /app
COPY --from=build /app/publish ./
ENV ASPNETCORE_URLS=http://0.0.0.0:8082
EXPOSE 8082
ENTRYPOINT ["dotnet", "EventsService.dll"]
