using Confluent.Kafka;

namespace WebhookService.Services
{
    public class KafkaProducerService : IDisposable
    {
        private readonly IProducer<Null, string> _producer;
        private readonly ILogger<KafkaProducerService> _logger;

        public KafkaProducerService(IConfiguration config, ILogger<KafkaProducerService> logger)
        {
            _logger = logger;
            var producerConfig = new ProducerConfig
            {
                BootstrapServers = config["Kafka:BootstrapServers"] ?? "localhost:9092",
                Acks = Acks.All
            };

            _producer = new ProducerBuilder<Null, string>(producerConfig).Build();
        }

        public async Task ProduceAsync(string topic, string message)
        {
            var deliveryResult = await _producer.ProduceAsync(topic, new Message<Null, string> { Value = message });
            _logger.LogInformation("Produced event to Kafka topic {Topic}. partition={Partition} offset={Offset}", topic, deliveryResult.Partition, deliveryResult.Offset);
        }

        public void Dispose()
        {
            _producer.Flush(TimeSpan.FromSeconds(5));
            _producer.Dispose();
        }
    }
}
