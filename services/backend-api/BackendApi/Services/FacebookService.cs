using BackendApi.Models;
using System.Text.Json;

namespace BackendApi.Services
{
    public interface IFacebookService
    {
        Task<ApiResponse<object>> GetPostsAsync();
        Task<ApiResponse<object>> CreatePostAsync(string message);
        Task<ApiResponse<object>> GetCommentsAsync(string postId);
    }

    public class FacebookService : IFacebookService
    {
        private readonly HttpClient _httpClient;
        private readonly IConfiguration _config;
        private readonly ILogger<FacebookService> _logger;

        public FacebookService(HttpClient httpClient, IConfiguration config, ILogger<FacebookService> logger)
        {
            _httpClient = httpClient;
            _config = config;
            _logger = logger;
        }

        public async Task<ApiResponse<object>> GetPostsAsync()
        {
            var pageId = _config["FacebookApi:PageId"];
            var token = _config["FacebookApi:AccessToken"];
            var url = $"{_config["FacebookApi:BaseUrl"]}/{pageId}/posts?access_token={token}";

            return await ExecuteRequestAsync(HttpMethod.Get, url);
        }

        public async Task<ApiResponse<object>> CreatePostAsync(string message)
        {
            var pageId = _config["FacebookApi:PageId"];
            var token = _config["FacebookApi:AccessToken"];
            var url = $"{_config["FacebookApi:BaseUrl"]}/{pageId}/feed?message={Uri.EscapeDataString(message)}&access_token={token}";

            return await ExecuteRequestAsync(HttpMethod.Post, url);
        }

        public async Task<ApiResponse<object>> GetCommentsAsync(string postId)
        {
            var token = _config["FacebookApi:AccessToken"];
            var url = $"{_config["FacebookApi:BaseUrl"]}/{postId}/comments?access_token={token}";

            return await ExecuteRequestAsync(HttpMethod.Get, url);
        }

        private async Task<ApiResponse<object>> ExecuteRequestAsync(HttpMethod method, string url)
        {
            try
            {
                var request = new HttpRequestMessage(method, url);
                _logger.LogInformation("Đang gửi {Method} request tới Facebook API lúc {Time}", method, DateTime.UtcNow);

                var response = await _httpClient.SendAsync(request);
                var content = await response.Content.ReadAsStringAsync();

                if (response.IsSuccessStatusCode)
                {
                    _logger.LogInformation("Request thành công với mã {StatusCode}.", (int)response.StatusCode);
                    return new ApiResponse<object>
                    {
                        Success = true,
                        ErrorCode = 200,
                        Message = "Thành công",
                        Data = JsonSerializer.Deserialize<object>(content)
                    };
                }

                _logger.LogError("Lỗi Facebook API. Status: {StatusCode}, Body: {Content}", response.StatusCode, content);
                return new ApiResponse<object>
                {
                    Success = false,
                    ErrorCode = (int)response.StatusCode,
                    Message = "Lỗi từ phía Facebook API. Vui lòng xem Data hoặc Log để biết chi tiết.",
                    Data = JsonSerializer.Deserialize<object>(content)
                };
            }
            catch (Exception ex)
            {
                _logger.LogCritical(ex, "Lỗi hệ thống khi gọi Facebook API.");
                return new ApiResponse<object>
                {
                    Success = false,
                    ErrorCode = 500,
                    Message = "Lỗi server nội bộ khi kết nối tới Facebook."
                };
            }
        }
    }
}