using Microsoft.AspNetCore.SignalR;

namespace rock_ctrl.Hubs;

public class TelemetryHub : Hub
{
    private readonly ILogger<TelemetryHub> _logger;

    public TelemetryHub(ILogger<TelemetryHub> logger)
    {
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));        
    }
    
    public override async Task OnConnectedAsync()
    {
        _logger.LogInformation("new client connected: {connectionId}", Context.ConnectionId);
        
        await Clients.Caller.SendAsync("TelemetryHubConnected", Context.ConnectionId);
    }
}