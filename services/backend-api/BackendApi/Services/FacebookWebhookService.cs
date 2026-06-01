using System.Net.Http.Headers;
using System.Text.Json;

namespace BackendApi.Services
{
    public class FacebookWebhookService
    {
        private readonly HttpClient _httpClient;
        private readonly IConfiguration _config;
        private readonly ILogger<FacebookWebhookService> _logger;

        public FacebookWebhookService(HttpClient httpClient, IConfiguration config, ILogger<FacebookWebhookService> logger)
        {
            _httpClient = httpClient;
            _config = config;
            _logger = logger;
        }

        /// <summary>
        /// Subscribe webhook fields cho Page
        /// </summary>
        public async Task<bool> SubscribeWebhookFieldsAsync(string pageId, string pageAccessToken)
        {
            try
            {
                // Các fields cần subscribe
                var fields = "feed,comments,conversations,message_echoes";

                // Endpoint
                var url = $"https://graph.facebook.com/v25.0/{pageId}/subscribed_apps";

                // Tham số
                var content = new FormUrlEncodedContent(new[]
                {
                    new KeyValuePair<string, string>("subscribed_fields", fields),
                    new KeyValuePair<string, string>("access_token", pageAccessToken)
                });

                var response = await _httpClient.PostAsync(url, content);

                if (response.IsSuccessStatusCode)
                {
                    var responseBody = await response.Content.ReadAsStringAsync();
                    _logger.LogInformation($"✅ Subscribe webhook fields thành công: {responseBody}");
                    return true;
                }
                else
                {
                    var errorBody = await response.Content.ReadAsStringAsync();
                    _logger.LogError($"❌ Lỗi subscribe webhook: {response.StatusCode} - {errorBody}");
                    return false;
                }
            }
            catch (Exception ex)
            {
                _logger.LogError($"❌ Exception subscribe webhook: {ex.Message}");
                return false;
            }
        }

        /// <summary>
        /// Kiểm tra webhook fields hiện tại đã subscribe
        /// </summary>
        public async Task<List<string>> GetSubscribedFieldsAsync(string pageId, string pageAccessToken)
        {
            try
            {
                var url = $"https://graph.facebook.com/v25.0/{pageId}/subscribed_apps?access_token={pageAccessToken}";

                var response = await _httpClient.GetAsync(url);

                if (response.IsSuccessStatusCode)
                {
                    var jsonContent = await response.Content.ReadAsStringAsync();
                    using var document = JsonDocument.Parse(jsonContent);
                    var root = document.RootElement;

                    if (root.TryGetProperty("data", out var dataArray) && dataArray.ValueKind == JsonValueKind.Array)
                    {
                        foreach (var item in dataArray.EnumerateArray())
                        {
                            if (item.TryGetProperty("subscribed_fields", out var fields))
                            {
                                var fieldList = new List<string>();
                                foreach (var field in fields.EnumerateArray())
                                {
                                    fieldList.Add(field.GetString());
                                }
                                _logger.LogInformation($"✅ Subscribed fields hiện tại: {string.Join(", ", fieldList)}");
                                return fieldList;
                            }
                        }
                    }
                }
                else
                {
                    var errorBody = await response.Content.ReadAsStringAsync();
                    _logger.LogError($"❌ Lỗi lấy subscribed fields: {response.StatusCode} - {errorBody}");
                }
            }
            catch (Exception ex)
            {
                _logger.LogError($"❌ Exception lấy subscribed fields: {ex.Message}");
            }

            return new List<string>();
        }
    }
}
