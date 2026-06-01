using Confluent.Kafka;
using System.Text.Json;
using System.Collections.Concurrent;
using CoreService.Models;
using System.Net.Http; // Đã thêm thư viện này để gọi API

namespace CoreService.Services
{
    public class KafkaConsumerService : BackgroundService
    {
        private readonly SentimentAnalysisService _aiService;
        private readonly IConfiguration _config;
        
        // GIẢ LẬP DATABASE LƯU ID ĐỂ CHECK IDEMPOTENT
        private static readonly ConcurrentDictionary<string, bool> _processedComments = new();

        public KafkaConsumerService(SentimentAnalysisService aiService, IConfiguration config)
        {
            _aiService = aiService;
            _config = config;
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            var config = new ConsumerConfig
            {
                BootstrapServers = _config["Kafka:BootstrapServers"] ?? "localhost:9092",
                GroupId = "core-service-group",
                AutoOffsetReset = AutoOffsetReset.Earliest
            };

            using var consumer = new ConsumerBuilder<Ignore, string>(config).Build();
            consumer.Subscribe("raw_events");

            Console.WriteLine("🚀 [Core Service] Đang lắng nghe dữ liệu từ Kafka topic 'raw_events'...");

            try
            {
                while (!stoppingToken.IsCancellationRequested)
                {
                    var consumeResult = consumer.Consume(stoppingToken);
                    string rawJson = consumeResult.Message.Value;

                    try
                    {
                        var fbEvent = JsonSerializer.Deserialize<FacebookWebhookEvent>(rawJson);
                        
                        // Lấy ra comment_id và nội dung
                        var valueNode = fbEvent?.entry?.FirstOrDefault()?.changes?.FirstOrDefault()?.value;
                        
                        if (valueNode != null && valueNode.verb == "add" && !string.IsNullOrEmpty(valueNode.comment_id))
                        {
                            string commentId = valueNode.comment_id;
                            string message = valueNode.message;

                            Console.WriteLine("\n--------------------------------------------------");
                            Console.WriteLine($"📥 Nhận comment mới: '{message}' (ID: {commentId})");

                            // ==========================================
                            // 1. KIỂM TRA IDEMPOTENT (CHỐNG TRÙNG LẶP)
                            // ==========================================
                            if (_processedComments.ContainsKey(commentId))
                            {
                                Console.WriteLine($"⚠️ [Idempotent] Bỏ qua comment {commentId} vì ĐÃ ĐƯỢC XỬ LÝ TRƯỚC ĐÓ. (Tránh gửi lệnh 2 lần)");
                                Console.WriteLine("--------------------------------------------------");
                                continue; // Chặn lại, không chạy tiếp
                            }

                            // Đánh dấu là đã xử lý
                            _processedComments.TryAdd(commentId, true);

                            // ==========================================
                            // 2. GỌI AI PHÂN TÍCH CẢM XÚC
                            // ==========================================
                            var (sentiment, action, replyMsg) = _aiService.Analyze(message);
                            
                            Console.WriteLine($"🤖 [AI] Phân loại cảm xúc: {sentiment}");
                            Console.WriteLine($"⚡ [Hành động] Quyết định: {action}");
                            if (!string.IsNullOrEmpty(replyMsg))
                            {
                                Console.WriteLine($"💬 [Nội dung phản hồi]: {replyMsg}");
                            }
                            
                            // ==========================================
                            // 3. ĐẨY LỆNH SANG BACKEND API ĐỂ GỬI FACEBOOK
                            // ==========================================
                            try 
                            {
                                using var http = new HttpClient();
                                // Gọi sang cổng 3002 của Backend API
                                await http.PostAsync("http://localhost:3002/test-retry", null);
                                Console.WriteLine("🚀 [Pipeline] Đã chuyển lệnh sang Backend API (Port 3002) để xử lý bảo vệ!");
                            }
                            catch (Exception httpEx)
                            {
                                Console.WriteLine($"❌ [Lỗi kết nối] Không thể gọi sang Backend API: {httpEx.Message}");
                            }
                            
                            Console.WriteLine("✅ Xử lý thành công!");
                            Console.WriteLine("--------------------------------------------------");
                        }
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine($"❌ Lỗi parse JSON: {ex.Message}");
                    }
                }
            }
            catch (OperationCanceledException)
            {
                consumer.Close();
            }
        }
    }
}