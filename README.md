# Rocket Control Center

Simple Rocket Launch Control Center. This is a back-end server that consumes rocket telemetry in a Kafka topic and send's the received data to the connected clients via SignalR.

# Run

To run the application:

```bash
dotnet run ./rock-ctrl.csproj
```

# Configurations

Configuration files:
- `appsettings.Development.json`
- `appsettings.json`