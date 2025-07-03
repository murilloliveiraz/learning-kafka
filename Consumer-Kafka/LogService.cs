using Confluent.Kafka;
using Consumer_Kafka.Interfaces;
using Consumer_Kafka.Services;

namespace Consumer_Kafka;

public class LogService : IConsumerFunction<string, string>
{
    public void Consume(ConsumeResult<string, string> record)
    {
        Console.WriteLine("---------------------");
        Console.WriteLine($"LOG DE EVENTO DO KAFKA:");
        Console.WriteLine($"  Tópico: {record.Topic}");
        Console.WriteLine($"  Key: {record.Message.Key}");
        Console.WriteLine($"  Value: {record.Message.Value}");
        Console.WriteLine($"  Partição: {record.Partition.Value}");
        Console.WriteLine($"  Offset: {record.Offset.Value}");
        Console.WriteLine($"  Timestamp: {record.Message.Timestamp.UtcDateTime}");
    }

    public async Task Start(CancellationToken cancellationToken)
    {
        using (var kafkaService = new KafkaService<string, string>(
            "LogService",
            "ECOMMERCE.*",
            this,
            Deserializers.Utf8,
            Deserializers.Utf8
            ))
        {
            await kafkaService.Run(cancellationToken);
        }
    }
}