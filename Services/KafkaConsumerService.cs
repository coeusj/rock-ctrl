using Confluent.Kafka;
using Microsoft.AspNetCore.SignalR;
using Microsoft.Extensions.Options;
using rock_ctrl.Hubs;
using rock_ctrl.Models;

namespace rock_ctrl.Services;

public class KafkaConsumerService : BackgroundService
{
    private readonly ILogger<KafkaConsumerService> _logger;
    private readonly KafkaSettings _kafkaSettings;
    private readonly IHubContext<TelemetryHub> _telemetryHubCtx;
    private readonly ConsumerConfig _kafkaConsumerConfig;

    public KafkaConsumerService(
        ILogger<KafkaConsumerService> logger,
        IHubContext<TelemetryHub> telemetryHubCtx,
        IOptions<KafkaSettings> kafkaSettings)
    {
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        _telemetryHubCtx = telemetryHubCtx ?? throw new ArgumentNullException(nameof(telemetryHubCtx));
        _kafkaSettings = kafkaSettings.Value;
        _kafkaConsumerConfig = new ConsumerConfig
        {
            GroupId = _kafkaSettings.GroupId,
            BootstrapServers = _kafkaSettings.BootstrapServers,
            AutoOffsetReset = AutoOffsetReset.Latest
        };
    }
    
    protected override Task ExecuteAsync(CancellationToken cancellationToken)
    {
        return Task.Run(async () => await ConsumeLoop(cancellationToken), cancellationToken);
    }

    private async Task ConsumeLoop(CancellationToken cancellationToken)
    {
        using var consumer = new ConsumerBuilder<string, string>(_kafkaConsumerConfig).Build();
        consumer.Subscribe(_kafkaSettings.Topic);

        try
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                try
                {
                    var consumeRes = consumer.Consume(TimeSpan.FromSeconds(3));
                    if (consumeRes == null)
                        continue;
                    
                    _logger.LogInformation("Consumed message: {msg}", consumeRes.Message.Value);
                    
                    await _telemetryHubCtx.Clients.All.SendAsync("NavigationMsg", consumeRes.Message.Value, cancellationToken);
                }
                catch (ConsumeException e)
                {
                    _logger.LogError(e, "Consume Exception");
                }
            }
        }
        catch (OperationCanceledException ex)
        {
            _logger.LogWarning("Consume cancelled: {exMessage}", ex.Message);
        }
        finally
        {
            consumer.Close();
            _logger.LogInformation("Consumer closed");
        }
    }
}