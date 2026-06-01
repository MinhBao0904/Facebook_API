using WebhookService.Services;

var builder = WebApplication.CreateBuilder(args);

// Port 3001
builder.WebHost.UseUrls("http://*:3001");

builder.Services.AddControllers();
// Cho phép đọc body stream nhiều lần (cần thiết cho Webhook)
builder.Services.Configure<Microsoft.AspNetCore.Http.Json.JsonOptions>(options => {});

builder.Services.AddSingleton<KafkaProducerService>();

var app = builder.Build();

app.Use(async (context, next) => {
    context.Request.EnableBuffering(); // Cho phép đọc body raw
    await next();
});

app.MapControllers();
app.Run();