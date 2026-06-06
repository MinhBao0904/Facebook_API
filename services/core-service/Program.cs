using CoreService.Services;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddSingleton<SentimentAnalysisService>();
builder.Services.AddHttpClient("backend-api", (serviceProvider, client) =>
{
    var config = serviceProvider.GetRequiredService<IConfiguration>();
    client.BaseAddress = new Uri(config["BackendApi:BaseUrl"] ?? "http://localhost:3002/");
    client.DefaultRequestHeaders.Add("X-API-KEY", config["BackendApi:ApiKey"] ?? "SECRET_KEY_123");
});
builder.Services.AddHostedService<KafkaConsumerService>();

var app = builder.Build();

app.MapGet("/", () => new
{
    service = "core-service",
    status = "running",
    consumes = "raw_events"
});

app.Run();
