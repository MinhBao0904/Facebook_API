using BackendApi.Services;

var builder = WebApplication.CreateBuilder(args);

// Ép chạy trên cổng 3000
builder.WebHost.UseUrls("http://*:3000");

// Thêm các dịch vụ cần thiết vào DI Container
builder.Services.AddControllers();
builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen();

builder.Services.AddHttpClient();
builder.Services.AddScoped<IFacebookService, FacebookService>();
builder.Services.AddScoped<FacebookWebhookService>();

var app = builder.Build();

// Cấu hình HTTP request pipeline
if (app.Environment.IsDevelopment())
{
    app.UseSwagger();
    app.UseSwaggerUI();
}

app.UseAuthorization();
app.MapControllers();

app.Run();