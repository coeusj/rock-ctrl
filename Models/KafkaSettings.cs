namespace rock_ctrl.Models;

public record KafkaSettings
{
    public required string BootstrapServers { get; init; }
    public required string Topic { get; init; }
    public required string GroupId { get; init; }
}