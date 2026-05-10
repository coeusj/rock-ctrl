using rock_ctrl.Hubs;
using rock_ctrl.Configurations;
using rock_ctrl.Services;

var builder = WebApplication.CreateBuilder(args);
builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen();
builder.Services.Configure<KafkaSettings>(builder.Configuration.GetSection("KafkaSettings"));
builder.Services.AddHostedService<KafkaConsumerService>();
builder.Services.AddSignalR();

builder.Services.AddCors(options => {
    options.AddPolicy("SignalRPolicy", policyBuilder =>
    {
        policyBuilder.WithOrigins("http://localhost:5173")
            .AllowAnyHeader()
            .AllowAnyMethod()
            .AllowCredentials();
    });
});

var app = builder.Build();
if (app.Environment.IsDevelopment())
{
    app.UseSwagger();
    app.UseSwaggerUI();
}

app.UseCors("SignalRPolicy");
app.UseHttpsRedirection();
app.MapHub<TelemetryHub>("/telemetry");
app.Run();