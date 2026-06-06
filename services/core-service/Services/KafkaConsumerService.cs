using System.Collections.Concurrent;
using System.Net.Http.Json;
using System.Text.Json;
using Confluent.Kafka;
using CoreService.Models;

namespace CoreService.Services
{
    public class KafkaConsumerService : BackgroundService
    {
        private readonly SentimentAnalysisService _aiService;
        private readonly IConfiguration _config;
        private readonly IHttpClientFactory _httpClientFactory;
        private readonly ILogger<KafkaConsumerService> _logger;
        private readonly ConcurrentDictionary<string, DateTimeOffset> _processedEvents = new();
        private readonly ConcurrentDictionary<string, Queue<DateTimeOffset>> _userEventWindows = new();

        public KafkaConsumerService(
            SentimentAnalysisService aiService,
            IConfiguration config,
            IHttpClientFactory httpClientFactory,
            ILogger<KafkaConsumerService> logger)
        {
            _aiService = aiService;
            _config = config;
            _httpClientFactory = httpClientFactory;
            _logger = logger;
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            var consumerConfig = new ConsumerConfig
            {
                BootstrapServers = _config["Kafka:BootstrapServers"] ?? "localhost:9092",
                GroupId = _config["Kafka:GroupId"] ?? "core-service-group",
                AutoOffsetReset = AutoOffsetReset.Earliest,
                EnableAutoCommit = false
            };

            using var consumer = new ConsumerBuilder<Ignore, string>(consumerConfig).Build();
            using var dlqProducer = new ProducerBuilder<Null, string>(new ProducerConfig
            {
                BootstrapServers = consumerConfig.BootstrapServers,
                Acks = Acks.All
            }).Build();

            var rawTopic = _config["Kafka:TopicRawEvents"] ?? "raw_events";
            consumer.Subscribe(rawTopic);
            _logger.LogInformation("Core Service is consuming Kafka topic {Topic}.", rawTopic);

            while (!stoppingToken.IsCancellationRequested)
            {
                ConsumeResult<Ignore, string>? result = null;

                try
                {
                    result = consumer.Consume(stoppingToken);
                    await ProcessMessageAsync(result.Message.Value, dlqProducer, stoppingToken);
                    consumer.Commit(result);
                }
                catch (OperationCanceledException)
                {
                    break;
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Failed to process Kafka message.");
                    if (result != null)
                    {
                        await PublishDeadLetterAsync(dlqProducer, result.Message.Value, "core_processing_failed", ex.Message, stoppingToken);
                        consumer.Commit(result);
                    }
                }
            }

            consumer.Close();
        }

        private async Task ProcessMessageAsync(string rawJson, IProducer<Null, string> dlqProducer, CancellationToken cancellationToken)
        {
            var fbEvent = JsonSerializer.Deserialize<NormalizedFacebookEvent>(rawJson);
            if (fbEvent == null || fbEvent.verb != "add" || string.IsNullOrWhiteSpace(fbEvent.comment_id))
            {
                _logger.LogInformation("Skipping unsupported event: {Payload}", rawJson);
                return;
            }

            var commandId = fbEvent.event_id ?? fbEvent.comment_id;
            if (!_processedEvents.TryAdd(commandId, DateTimeOffset.UtcNow))
            {
                _logger.LogWarning("Idempotent skip. command_id={CommandId}", commandId);
                return;
            }

            if (IsRateLimited(fbEvent.user_id ?? "unknown"))
            {
                _logger.LogWarning("User {UserId} is rate limited. Event moved to pending_review. command_id={CommandId}", fbEvent.user_id, commandId);
                return;
            }

            var analysis = _aiService.Analyze(fbEvent.message);
            _logger.LogInformation(
                "Event processed. command_id={CommandId} comment_id={CommentId} sentiment={Sentiment} intent={Intent} action={Action}",
                commandId,
                fbEvent.comment_id,
                analysis.Sentiment,
                analysis.Intent,
                analysis.Action);

            if (analysis.Action == "skip")
            {
                return;
            }

            if (analysis.Action == "review")
            {
                _logger.LogWarning("Severe spam requires manual review. command_id={CommandId}", commandId);
                await SendAutomationCommandAsync("hide", fbEvent.comment_id, commandId, "", "severe_spam_review", cancellationToken);
                return;
            }

            if (analysis.Action == "hide")
            {
                await SendAutomationCommandAsync("hide", fbEvent.comment_id, commandId, "", analysis.Intent, cancellationToken);
                return;
            }

            if (analysis.Action == "reply")
            {
                await SendAutomationCommandAsync("reply", fbEvent.comment_id, commandId, analysis.ReplyMessage, analysis.Intent, cancellationToken);
            }
        }

        private bool IsRateLimited(string userId)
        {
            var now = DateTimeOffset.UtcNow;
            var window = _userEventWindows.GetOrAdd(userId, _ => new Queue<DateTimeOffset>());

            lock (window)
            {
                while (window.Count > 0 && now - window.Peek() > TimeSpan.FromMinutes(1))
                {
                    window.Dequeue();
                }

                window.Enqueue(now);
                return window.Count > 20;
            }
        }

        private async Task SendAutomationCommandAsync(
            string action,
            string commentId,
            string commandId,
            string replyMessage,
            string reason,
            CancellationToken cancellationToken)
        {
            var client = _httpClientFactory.CreateClient("backend-api");
            var payload = new
            {
                commandId,
                commentId,
                action,
                replyMessage,
                reason
            };

            var endpoint = action == "reply" ? "automation/reply" : "automation/hide";
            var response = await client.PostAsJsonAsync(endpoint, payload, cancellationToken);

            if (!response.IsSuccessStatusCode)
            {
                var body = await response.Content.ReadAsStringAsync(cancellationToken);
                throw new InvalidOperationException($"Backend automation failed. status={(int)response.StatusCode}, body={body}");
            }
        }

        private async Task PublishDeadLetterAsync(
            IProducer<Null, string> producer,
            string originalPayload,
            string errorType,
            string reason,
            CancellationToken cancellationToken)
        {
            var topic = _config["Kafka:TopicDeadLetter"] ?? "dead_letter";
            var payload = JsonSerializer.Serialize(new
            {
                error_type = errorType,
                reason,
                original_payload = originalPayload,
                failed_at = DateTimeOffset.UtcNow
            });

            await producer.ProduceAsync(topic, new Message<Null, string> { Value = payload }, cancellationToken);
            _logger.LogCritical("Published failed message to DLQ topic {Topic}.", topic);
        }
    }
}
