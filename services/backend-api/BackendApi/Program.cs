using BackendApi.Services;

var builder = WebApplication.CreateBuilder(args);

// Đăng ký Service
builder.Services.AddSingleton<FacebookApiService>();

// Chạy API ở port 3002 để không đụng hàng với Webhook (3001)
builder.WebHost.UseUrls("http://*:3002");

var app = builder.Build();

// Tạo 1 endpoint để Postman gọi vào test
app.MapPost("/test-retry", async (FacebookApiService fbService) =>
{
    await fbService.SendReplyAsync("Cảm ơn bạn đã ủng hộ shop!");
    return Results.Ok("Đã nhận lệnh kích hoạt test API");
});

app.Run();