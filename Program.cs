using rock_ctrl.Hubs;
using rock_ctrl.Models;
using rock_ctrl.Services;

var builder = WebApplication.CreateBuilder(args);
builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen();
builder.Services.Configure<KafkaSettings>(builder.Configuration.GetSection("KafkaSettings"));
builder.Services.AddHostedService<KafkaConsumerService>();
builder.Services.AddSignalR();

var app = builder.Build();
if (app.Environment.IsDevelopment())
{
    app.UseSwagger();
    app.UseSwaggerUI();
}

app.UseHttpsRedirection();
app.MapHub<TelemetryHub>("/telemetry");
app.Run();