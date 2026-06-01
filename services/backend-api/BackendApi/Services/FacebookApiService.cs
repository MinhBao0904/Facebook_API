using System;
using System.Net.Http;
using System.Threading.Tasks;
using Polly;
using Polly.CircuitBreaker;
using Polly.Retry;
using Confluent.Kafka;
using Microsoft.Extensions.Configuration;

namespace BackendApi.Services
{
    public class FacebookApiService
    {
        private readonly AsyncRetryPolicy _retryPolicy;
        private readonly AsyncCircuitBreakerPolicy _circuitBreakerPolicy;
        private readonly HttpClient _httpClient;
        private readonly IConfiguration _config;

        public FacebookApiService(IConfiguration config)
        {
            _config = config;
            _httpClient = new HttpClient();
            _httpClient.BaseAddress = new Uri("http://localhost:9999"); // Link chết để ép lỗi

            _retryPolicy = Policy
                .Handle<Exception>()
                .WaitAndRetryAsync(
                    3,
                    retryAttempt => TimeSpan.FromSeconds(Math.Pow(2, retryAttempt - 1)),
                    onRetry: (exception, timeSpan, retryCount, context) =>
                    {
                        Console.WriteLine($"⚠️ [Retry] Gọi API lỗi. Thử lại lần {retryCount} sau {timeSpan.TotalSeconds}s...");
                    });

            _circuitBreakerPolicy = Policy
                .Handle<Exception>()
                .CircuitBreakerAsync(
                    exceptionsAllowedBeforeBreaking: 5,
                    durationOfBreak: TimeSpan.FromSeconds(30),
                    onBreak: (exception, timespan) =>
                    {
                        Console.WriteLine($"\n🚨 [Circuit Breaker] Gọi API thất bại 5 lần liên tiếp. MẠCH ĐÃ MỞ (OPEN).");
                    },
                    onReset: () => Console.WriteLine("🟢 [Circuit Breaker] Mạch đã đóng lại (CLOSED)."),
                    onHalfOpen: () => Console.WriteLine("🟡 [Circuit Breaker] Mạch đang thử nghiệm lại (HALF-OPEN)...")
                );
        }

        public async Task SendReplyAsync(string message)
        {
            var policyWrap = Policy.WrapAsync(_circuitBreakerPolicy, _retryPolicy);

            try
            {
                await policyWrap.ExecuteAsync(async () =>
                {
                    Console.WriteLine($"\n🚀 Đang gửi API lên Facebook nội dung: '{message}'...");
                    var response = await _httpClient.GetAsync("/fake-facebook-endpoint");
                    response.EnsureSuccessStatusCode();
                });
            }
            catch (BrokenCircuitException)
            {
                Console.WriteLine("❌ [Blocked] Mạch đang MỞ, request bị chặn lại ngay lập tức.");
            }
            catch (Exception)
            {
                Console.WriteLine($"❌ [Failed] Đã hết số lần Retry nhưng vẫn lỗi.");
                
                // ==========================================
                // ĐẨY MESSAGE VÀO DEAD LETTER QUEUE (DLQ)
                // ==========================================
                await SendToDeadLetterQueue(message, "Lỗi gọi Facebook API sau 3 lần Retry");
            }
        }

        private async Task SendToDeadLetterQueue(string originalMessage, string reason)
        {
            var kafkaConfig = new ProducerConfig { BootstrapServers = _config["Kafka:BootstrapServers"] ?? "localhost:9092" };
            using var producer = new ProducerBuilder<Null, string>(kafkaConfig).Build();

            var dlqMessage = $"{{\"message\": \"{originalMessage}\", \"error_reason\": \"{reason}\", \"timestamp\": \"{DateTime.UtcNow}\"}}";

            try
            {
                var result = await producer.ProduceAsync("dead_letter", new Message<Null, string> { Value = dlqMessage });
                Console.WriteLine($"\n💀 [DLQ] Đã lưu message thất bại vào topic 'dead_letter'.");
                Console.WriteLine($"🔔 [Alert] Kích hoạt Prometheus/Alertmanager cảnh báo vận hành!");
            }
            catch (ProduceException<Null, string> e)
            {
                Console.WriteLine($"Delivery failed: {e.Error.Reason}");
            }
        }
    }
}