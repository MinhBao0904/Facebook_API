using Confluent.Kafka;

namespace WebhookService.Services
{
    public class KafkaProducerService
    {
        private readonly IProducer<Null, string> _producer;
        private readonly ILogger<KafkaProducerService> _logger;
        private readonly string _topic;

        public KafkaProducerService(IConfiguration config, ILogger<KafkaProducerService> logger)
        {
            _logger = logger;
            _topic = config["Kafka:TopicRawEvents"] ?? "raw_events";
            
            var producerConfig = new ProducerConfig { BootstrapServers = config["Kafka:BootstrapServers"] };
            _producer = new ProducerBuilder<Null, string>(producerConfig).Build();
        }

        public async Task ProduceAsync(string topic, string message)
        {
            try
            {
                var deliveryResult = await _producer.ProduceAsync(topic, new Message<Null, string> { Value = message });
                _logger.LogInformation($"Đã đẩy event vào Kafka topic {topic} | Partition: {deliveryResult.Partition} | Offset: {deliveryResult.Offset}");
            }
            catch (ProduceException<Null, string> e)
            {
                _logger.LogError($"Lỗi khi đẩy vào Kafka: {e.Error.Reason}");
            }
        }
    }
}