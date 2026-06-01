using CoreService.Services;

var builder = WebApplication.CreateBuilder(args);

// BƯỚC 1: ĐĂNG KÝ CÁC DỊCH VỤ (DEPENDENCY INJECTION)

// 1. Đăng ký AI phân tích cảm xúc
builder.Services.AddSingleton<SentimentAnalysisService>();

// 2. Đăng ký Kafka Consumer chạy ngầm (Background Service)
builder.Services.AddHostedService<KafkaConsumerService>();


var app = builder.Build();

// Tạo một endpoint nhỏ để bạn dễ test trên trình duyệt xem service có đang bật không
app.MapGet("/", () => "🚀 Core Service đang hoạt động và lắng nghe Kafka!");

app.Run();