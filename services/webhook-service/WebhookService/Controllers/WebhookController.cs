using Microsoft.AspNetCore.Mvc;
using System.IO;
using System.Text;
using System.Threading.Tasks;
using System.Security.Cryptography;
using WebhookService.Services;

namespace WebhookService.Controllers
{
    [ApiController]
    [Route("[controller]")] // Đường dẫn sẽ là /webhook
    public class WebhookController : ControllerBase
    {
        private readonly KafkaProducerService _kafkaProducer;
        private readonly IConfiguration _config;

        public WebhookController(KafkaProducerService kafkaProducer, IConfiguration config)
        {
            _kafkaProducer = kafkaProducer;
            _config = config;
        }

        // BƯỚC 1: XÁC MINH WEBHOOK (GET)
        [HttpGet]
        public IActionResult VerifyWebhook([FromQuery(Name = "hub.mode")] string mode,
                                           [FromQuery(Name = "hub.challenge")] string challenge,
                                           [FromQuery(Name = "hub.verify_token")] string token)
        {
            // Đọc Verify Token từ appsettings.json
            string myVerifyToken = _config["Facebook:VerifyToken"] ?? "my_secret_verify_token_123"; 

            if (mode == "subscribe" && token == myVerifyToken)
            {
                Console.WriteLine("✅ Xác minh Webhook thành công!");
                return Ok(challenge); // Bắt buộc trả về challenge bằng HTTP 200
            }
            
            Console.WriteLine("❌ Xác minh Webhook thất bại! Sai Token.");
            return Forbid();
        }

        // BƯỚC 2: NHẬN SỰ KIỆN TỪ FACEBOOK (POST)
        [HttpPost]
        public async Task<IActionResult> ReceiveWebhook()
        {
            try
            {
                // Đọc toàn bộ dữ liệu thô (raw JSON) Facebook gửi đến
                using StreamReader reader = new StreamReader(Request.Body, Encoding.UTF8);
                string rawBody = await reader.ReadToEndAsync();

                // KIỂM TRA CHỮ KÝ TỬ FACEBOOK (Tùy chọn nhưng KHUYÊN DÙNG)
                // if (!VerifyWebhookSignature(rawBody))
                // {
                //     Console.WriteLine("❌ Chữ ký Webhook không hợp lệ! Có thể không phải từ Facebook.");
                //     return Unauthorized();
                // }

                Console.WriteLine("\n==================================================");
                Console.WriteLine("📥 CÓ DỮ LIỆU TỪ FACEBOOK GỬI VỀ:");
                Console.WriteLine(rawBody);
                Console.WriteLine("==================================================\n");

                // Đẩy vào Kafka
                await _kafkaProducer.ProduceAsync("raw_events", rawBody);
                Console.WriteLine("✅ Đã đẩy event vào Kafka topic raw_events");

                // Facebook yêu cầu phải trả về "200 OK" trong vòng 20 giây
                return Ok("EVENT_RECEIVED");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"❌ Lỗi xử lý Webhook: {ex.Message}");
                // Vẫn nên trả về 200 OK để Facebook không gửi lại liên tục khi code mình lỗi
                return Ok();
            }
        }

        // Xác thực chữ ký X-Hub-Signature từ Facebook
        private bool VerifyWebhookSignature(string body)
        {
            try
            {
                // Lấy App Secret từ config
                string appSecret = _config["Facebook:AppSecret"];
                if (string.IsNullOrEmpty(appSecret))
                {
                    Console.WriteLine("⚠️ Cảnh báo: AppSecret không được cấu hình!");
                    return true; // Cho pass nếu không có AppSecret (tạm thời)
                }

                // Lấy X-Hub-Signature header từ request
                if (!Request.Headers.TryGetValue("X-Hub-Signature-256", out var signature))
                {
                    Console.WriteLine("⚠️ Cảnh báo: Header X-Hub-Signature-256 không tìm thấy!");
                    return false;
                }

                // Tính HMAC SHA256
                using (var hmac = new HMACSHA256(Encoding.UTF8.GetBytes(appSecret)))
                {
                    string hash = "sha256=" + BitConverter.ToString(hmac.ComputeHash(Encoding.UTF8.GetBytes(body)))
                        .Replace("-", "")
                        .ToLower();

                    bool isValid = hash == signature.ToString().ToLower();
                    if (isValid)
                    {
                        Console.WriteLine("✅ Chữ ký Webhook hợp lệ - Từ Facebook");
                    }
                    else
                    {
                        Console.WriteLine($"❌ Chữ ký không hợp lệ. Kỳ vọng: {hash}, Nhận: {signature}");
                    }
                    return isValid;
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"❌ Lỗi xác thực chữ ký: {ex.Message}");
                return false;
            }
        }
    }
}