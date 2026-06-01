using Microsoft.AspNetCore.Mvc;
using BackendApi.Services;

namespace BackendApi.Controllers
{
    [ApiController]
    [Route("api/[controller]")]
    public class WebhookConfigController : ControllerBase
    {
        private readonly FacebookWebhookService _webhookService;
        private readonly IConfiguration _config;
        private readonly ILogger<WebhookConfigController> _logger;

        public WebhookConfigController(FacebookWebhookService webhookService, IConfiguration config, ILogger<WebhookConfigController> logger)
        {
            _webhookService = webhookService;
            _config = config;
            _logger = logger;
        }

        /// <summary>
        /// Subscribe webhook fields - Đăng ký nhận sự kiện từ Facebook
        /// GET: /api/webhookconfig/subscribe?pageId={pageId}&pageAccessToken={token}
        /// </summary>
        [HttpGet("subscribe")]
        public async Task<IActionResult> SubscribeWebhookFields([FromQuery] string pageId, [FromQuery] string pageAccessToken)
        {
            if (string.IsNullOrEmpty(pageId) || string.IsNullOrEmpty(pageAccessToken))
            {
                return BadRequest(new { error = "pageId và pageAccessToken là bắt buộc" });
            }

            var success = await _webhookService.SubscribeWebhookFieldsAsync(pageId, pageAccessToken);

            if (success)
            {
                return Ok(new 
                { 
                    message = "✅ Đã subscribe webhook fields thành công",
                    fields = "feed, comments, conversations, message_echoes"
                });
            }
            else
            {
                return BadRequest(new { error = "❌ Lỗi khi subscribe webhook" });
            }
        }

        /// <summary>
        /// Kiểm tra webhook fields hiện tại
        /// GET: /api/webhookconfig/check?pageId={pageId}&pageAccessToken={token}
        /// </summary>
        [HttpGet("check")]
        public async Task<IActionResult> CheckSubscribedFields([FromQuery] string pageId, [FromQuery] string pageAccessToken)
        {
            if (string.IsNullOrEmpty(pageId) || string.IsNullOrEmpty(pageAccessToken))
            {
                return BadRequest(new { error = "pageId và pageAccessToken là bắt buộc" });
            }

            var fields = await _webhookService.GetSubscribedFieldsAsync(pageId, pageAccessToken);

            return Ok(new 
            { 
                pageId,
                subscribedFields = fields,
                message = fields.Contains("comments") 
                    ? "✅ Field 'comments' đã được subscribe" 
                    : "❌ Field 'comments' chưa được subscribe"
            });
        }
    }
}
