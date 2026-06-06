using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using Microsoft.AspNetCore.Mvc;
using WebhookService.Services;

namespace WebhookService.Controllers
{
    [ApiController]
    [Route("[controller]")]
    public class WebhookController : ControllerBase
    {
        private readonly KafkaProducerService _kafkaProducer;
        private readonly IConfiguration _config;
        private readonly ILogger<WebhookController> _logger;

        public WebhookController(KafkaProducerService kafkaProducer, IConfiguration config, ILogger<WebhookController> logger)
        {
            _kafkaProducer = kafkaProducer;
            _config = config;
            _logger = logger;
        }

        [HttpGet]
        public IActionResult VerifyWebhook(
            [FromQuery(Name = "hub.mode")] string mode,
            [FromQuery(Name = "hub.challenge")] string challenge,
            [FromQuery(Name = "hub.verify_token")] string token)
        {
            var verifyToken = _config["Facebook:VerifyToken"];
            if (mode == "subscribe" && !string.IsNullOrWhiteSpace(verifyToken) && token == verifyToken)
            {
                _logger.LogInformation("Facebook webhook verification succeeded.");
                return Content(challenge, "text/plain");
            }

            _logger.LogWarning("Facebook webhook verification failed.");
            return Forbid();
        }

        [HttpPost]
        public async Task<IActionResult> ReceiveWebhook()
        {
            using var reader = new StreamReader(Request.Body, Encoding.UTF8);
            var rawBody = await reader.ReadToEndAsync();

            if (!VerifyWebhookSignature(rawBody))
            {
                return Unauthorized(new { success = false, errorCode = 401, message = "Invalid Facebook webhook signature." });
            }

            var normalizedEvents = NormalizeEvents(rawBody);
            foreach (var normalizedEvent in normalizedEvents)
            {
                await _kafkaProducer.ProduceAsync(
                    _config["Kafka:TopicRawEvents"] ?? "raw_events",
                    JsonSerializer.Serialize(normalizedEvent));
            }

            _logger.LogInformation("Received Facebook webhook. normalized_event_count={Count}", normalizedEvents.Count);
            return Ok("EVENT_RECEIVED");
        }

        private bool VerifyWebhookSignature(string body)
        {
            var appSecret = _config["Facebook:AppSecret"];
            if (string.IsNullOrWhiteSpace(appSecret))
            {
                _logger.LogWarning("Facebook AppSecret is missing. Signature verification is disabled for local development.");
                return true;
            }

            if (!Request.Headers.TryGetValue("X-Hub-Signature-256", out var providedSignature))
            {
                _logger.LogWarning("Missing X-Hub-Signature-256 header.");
                return false;
            }

            using var hmac = new HMACSHA256(Encoding.UTF8.GetBytes(appSecret));
            var hashBytes = hmac.ComputeHash(Encoding.UTF8.GetBytes(body));
            var expected = "sha256=" + Convert.ToHexString(hashBytes).ToLowerInvariant();
            var actual = providedSignature.ToString().Trim().ToLowerInvariant();

            var expectedBytes = Encoding.UTF8.GetBytes(expected);
            var actualBytes = Encoding.UTF8.GetBytes(actual);

            return expectedBytes.Length == actualBytes.Length &&
                   CryptographicOperations.FixedTimeEquals(expectedBytes, actualBytes);
        }

        private static List<object> NormalizeEvents(string rawBody)
        {
            var events = new List<object>();
            using var document = JsonDocument.Parse(rawBody);
            var root = document.RootElement;

            if (!root.TryGetProperty("entry", out var entries) || entries.ValueKind != JsonValueKind.Array)
            {
                return events;
            }

            foreach (var entry in entries.EnumerateArray())
            {
                var pageId = entry.TryGetProperty("id", out var entryId) ? entryId.GetString() : null;

                if (!entry.TryGetProperty("changes", out var changes) || changes.ValueKind != JsonValueKind.Array)
                {
                    continue;
                }

                foreach (var change in changes.EnumerateArray())
                {
                    var field = change.TryGetProperty("field", out var fieldElement) ? fieldElement.GetString() : "unknown";
                    if (!change.TryGetProperty("value", out var value))
                    {
                        continue;
                    }

                    var commentId = GetString(value, "comment_id") ?? GetString(value, "id");
                    var message = GetString(value, "message") ?? GetString(value, "text") ?? "";
                    var userId = GetNestedString(value, "from", "id") ?? GetString(value, "sender_id") ?? "unknown";
                    var verb = GetString(value, "verb") ?? "add";

                    events.Add(new
                    {
                        event_id = commentId ?? Guid.NewGuid().ToString("N"),
                        event_type = field,
                        page_id = pageId,
                        comment_id = commentId,
                        user_id = userId,
                        message,
                        verb,
                        received_at = DateTimeOffset.UtcNow,
                        raw = JsonSerializer.Deserialize<object>(value.GetRawText())
                    });
                }
            }

            return events;
        }

        private static string? GetString(JsonElement element, string propertyName)
        {
            return element.TryGetProperty(propertyName, out var value) && value.ValueKind != JsonValueKind.Null
                ? value.ToString()
                : null;
        }

        private static string? GetNestedString(JsonElement element, string objectName, string propertyName)
        {
            if (!element.TryGetProperty(objectName, out var nested) || nested.ValueKind != JsonValueKind.Object)
            {
                return null;
            }

            return GetString(nested, propertyName);
        }
    }
}
