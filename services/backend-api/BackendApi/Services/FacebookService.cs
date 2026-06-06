using System.Net;
using System.Text.Json;
using BackendApi.Models;
using Confluent.Kafka;
using Polly;
using Polly.CircuitBreaker;
using Polly.Wrap;

namespace BackendApi.Services
{
    public interface IFacebookService
    {
        Task<ApiResponse<object>> GetPostsAsync();
        Task<ApiResponse<object>> CreatePostAsync(string message);
        Task<ApiResponse<object>> GetCommentsAsync(string postId);
        Task<ApiResponse<object>> ReplyToCommentAsync(string commandId, string commentId, string message);
        Task<ApiResponse<object>> HideCommentAsync(string commandId, string commentId, string reason);
    }

    public class FacebookService : IFacebookService
    {
        private readonly HttpClient _httpClient;
        private readonly IConfiguration _config;
        private readonly ILogger<FacebookService> _logger;
        private readonly AsyncPolicyWrap<HttpResponseMessage> _facebookPolicy;

        public FacebookService(HttpClient httpClient, IConfiguration config, ILogger<FacebookService> logger)
        {
            _httpClient = httpClient;
            _config = config;
            _logger = logger;

            var retryPolicy = Policy<HttpResponseMessage>
                .Handle<HttpRequestException>()
                .Or<TaskCanceledException>()
                .OrResult(response => IsTransient(response.StatusCode))
                .WaitAndRetryAsync(
                    3,
                    retryAttempt => TimeSpan.FromSeconds(Math.Pow(2, retryAttempt - 1)),
                    (outcome, delay, retryCount, _) =>
                    {
                        var status = outcome.Result?.StatusCode.ToString() ?? outcome.Exception?.GetType().Name;
                        _logger.LogWarning("Facebook request failed with {Status}. Retry {RetryCount} after {Delay}s.", status, retryCount, delay.TotalSeconds);
                    });

            var circuitBreakerPolicy = Policy<HttpResponseMessage>
                .Handle<HttpRequestException>()
                .Or<TaskCanceledException>()
                .OrResult(response => IsTransient(response.StatusCode))
                .CircuitBreakerAsync(
                    handledEventsAllowedBeforeBreaking: 5,
                    durationOfBreak: TimeSpan.FromSeconds(30),
                    onBreak: (outcome, breakDelay) =>
                    {
                        var status = outcome.Result?.StatusCode.ToString() ?? outcome.Exception?.GetType().Name;
                        _logger.LogCritical("Facebook circuit breaker OPEN after repeated failures ({Status}). Break for {Seconds}s.", status, breakDelay.TotalSeconds);
                    },
                    onReset: () => _logger.LogInformation("Facebook circuit breaker CLOSED."),
                    onHalfOpen: () => _logger.LogWarning("Facebook circuit breaker HALF-OPEN."));

            _facebookPolicy = Policy.WrapAsync(circuitBreakerPolicy, retryPolicy);
        }

        public Task<ApiResponse<object>> GetPostsAsync()
        {
            var pageId = RequiredConfig("FacebookApi:PageId");
            var token = RequiredConfig("FacebookApi:AccessToken");
            var url = $"{BaseUrl()}/{pageId}/posts?fields=id,message,created_time,permalink_url&access_token={Uri.EscapeDataString(token)}";

            return ExecuteFacebookRequestAsync(HttpMethod.Get, url, "get_posts");
        }

        public Task<ApiResponse<object>> CreatePostAsync(string message)
        {
            var pageId = RequiredConfig("FacebookApi:PageId");
            var token = RequiredConfig("FacebookApi:AccessToken");
            var url = $"{BaseUrl()}/{pageId}/feed";
            return ExecuteFacebookRequestAsync(HttpMethod.Post, url, "create_post", () => new FormUrlEncodedContent(new[]
            {
                new KeyValuePair<string, string>("message", message),
                new KeyValuePair<string, string>("access_token", token)
            }));
        }

        public Task<ApiResponse<object>> GetCommentsAsync(string postId)
        {
            var token = RequiredConfig("FacebookApi:AccessToken");
            var url = $"{BaseUrl()}/{postId}/comments?fields=id,message,from,created_time&access_token={Uri.EscapeDataString(token)}";

            return ExecuteFacebookRequestAsync(HttpMethod.Get, url, "get_comments");
        }

        public Task<ApiResponse<object>> ReplyToCommentAsync(string commandId, string commentId, string message)
        {
            var token = RequiredConfig("FacebookApi:AccessToken");
            var url = $"{BaseUrl()}/{commentId}/comments";
            return ExecuteFacebookRequestAsync(HttpMethod.Post, url, "reply_comment", () => new FormUrlEncodedContent(new[]
            {
                new KeyValuePair<string, string>("message", message),
                new KeyValuePair<string, string>("access_token", token)
            }), commandId);
        }

        public Task<ApiResponse<object>> HideCommentAsync(string commandId, string commentId, string reason)
        {
            var token = RequiredConfig("FacebookApi:AccessToken");
            var url = $"{BaseUrl()}/{commentId}";
            return ExecuteFacebookRequestAsync(HttpMethod.Post, url, $"hide_comment:{reason}", () => new FormUrlEncodedContent(new[]
            {
                new KeyValuePair<string, string>("is_hidden", "true"),
                new KeyValuePair<string, string>("access_token", token)
            }), commandId);
        }

        private async Task<ApiResponse<object>> ExecuteFacebookRequestAsync(
            HttpMethod method,
            string url,
            string operation,
            Func<HttpContent?>? contentFactory = null,
            string? commandId = null)
        {
            try
            {
                _logger.LogInformation("Sending Facebook {Operation} request at {Time}.", operation, DateTimeOffset.UtcNow);

                var response = await _facebookPolicy.ExecuteAsync(async () =>
                {
                    using var request = new HttpRequestMessage(method, url);
                    var content = contentFactory?.Invoke();
                    if (content != null)
                    {
                        request.Content = content;
                    }

                    return await _httpClient.SendAsync(request);
                });

                var body = await response.Content.ReadAsStringAsync();
                _logger.LogInformation("Facebook {Operation} returned {StatusCode}. Body preview: {Preview}", operation, (int)response.StatusCode, Preview(body));

                if (response.IsSuccessStatusCode)
                {
                    return ApiResponse(true, 200, "Success", body);
                }

                var statusCode = (int)response.StatusCode;
                var message = statusCode is 400 or 401 or 403
                    ? "Facebook rejected the request. Check token, permissions, or input data."
                    : "Facebook API returned an error after retry policy.";

                if (commandId != null && IsTransient(response.StatusCode))
                {
                    await SendToDeadLetterQueueAsync(commandId, operation, body);
                }

                return ApiResponse(false, statusCode, message, body);
            }
            catch (BrokenCircuitException ex)
            {
                _logger.LogError(ex, "Facebook circuit breaker is open for operation {Operation}.", operation);
                if (commandId != null)
                {
                    await SendToDeadLetterQueueAsync(commandId, operation, "Circuit breaker is open");
                }

                return new ApiResponse<object>
                {
                    Success = false,
                    ErrorCode = 503,
                    Message = "Facebook API is temporarily blocked by circuit breaker."
                };
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Facebook {Operation} failed.", operation);
                if (commandId != null)
                {
                    await SendToDeadLetterQueueAsync(commandId, operation, ex.Message);
                }

                return new ApiResponse<object>
                {
                    Success = false,
                    ErrorCode = 500,
                    Message = "Internal server error while calling Facebook API."
                };
            }
        }

        private static bool IsTransient(HttpStatusCode statusCode)
        {
            return statusCode == HttpStatusCode.RequestTimeout
                || statusCode == (HttpStatusCode)429
                || (int)statusCode >= 500;
        }

        private ApiResponse<object> ApiResponse(bool success, int code, string message, string body)
        {
            object? data = null;
            if (!string.IsNullOrWhiteSpace(body))
            {
                try
                {
                    data = JsonSerializer.Deserialize<object>(body);
                }
                catch (JsonException)
                {
                    data = body;
                }
            }

            return new ApiResponse<object>
            {
                Success = success,
                ErrorCode = code,
                Message = message,
                Data = data
            };
        }

        private async Task SendToDeadLetterQueueAsync(string commandId, string operation, string reason)
        {
            var bootstrapServers = _config["Kafka:BootstrapServers"] ?? "localhost:9092";
            var topic = _config["Kafka:TopicDeadLetter"] ?? "dead_letter";
            var payload = JsonSerializer.Serialize(new
            {
                command_id = commandId,
                operation,
                reason,
                failed_at = DateTimeOffset.UtcNow
            });

            using var producer = new ProducerBuilder<Null, string>(new ProducerConfig { BootstrapServers = bootstrapServers }).Build();
            await producer.ProduceAsync(topic, new Message<Null, string> { Value = payload });
            _logger.LogCritical("Message moved to DLQ topic {Topic}. command_id={CommandId}", topic, commandId);
        }

        private string BaseUrl() => _config["FacebookApi:BaseUrl"] ?? "https://graph.facebook.com/v19.0";

        private string RequiredConfig(string key)
        {
            var value = _config[key];
            if (string.IsNullOrWhiteSpace(value))
            {
                throw new InvalidOperationException($"Missing configuration: {key}");
            }

            return value;
        }

        private static string Preview(string value)
        {
            if (string.IsNullOrEmpty(value))
            {
                return "";
            }

            return value.Length <= 500 ? value : value[..500];
        }
    }
}
