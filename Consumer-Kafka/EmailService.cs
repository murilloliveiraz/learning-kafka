using Confluent.Kafka;
using Consumer_Kafka.Helpers;
using Consumer_Kafka.Interfaces;
using Consumer_Kafka.Models;
using Consumer_Kafka.Services;
using static Confluent.Kafka.ConfigPropertyNames;

namespace Consumer_Kafka;

public class EmailService : IConsumerFunction<string, string>
{
    public void Consume(ConsumeResult<string, string> record)
    {
        Console.WriteLine("---------------------");
        Console.WriteLine("Processando nova ordem, enviando email");
        Console.WriteLine($"Key: {record.Message.Key}");
        Console.WriteLine($"Value: {record.Message.Value}");
        Console.WriteLine($"Partição: {record.Partition.Value}");
        Console.WriteLine($"Offset: {record.Offset.Value}");
        Console.WriteLine($"Tópico: {record.Topic}");

        try
        {
            Thread.Sleep(5000);
        }
        catch (ThreadInterruptedException e)
        {
            Console.WriteLine($"EmailService: Interrupção durante o sleep: {e.Message}");
        }

        Console.WriteLine("EmailService: Order processed (email send completed)");
    }

    public async Task Start(CancellationToken cancellationToken)
    {
        using (var kafkaService = new KafkaService<string, string>(
            "EmailService",
            "ECOMMERCE_SEND_EMAIL",
            this,
            Deserializers.Utf8,
            Deserializers.Utf8
            ))
        {
            await kafkaService.Run(cancellationToken);
        }
    }
}