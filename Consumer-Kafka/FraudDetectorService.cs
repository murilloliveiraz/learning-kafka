using Confluent.Kafka;
using Consumer_Kafka.Interfaces;
using Consumer_Kafka.Services;

namespace Consumer_Kafka
{
    public class FraudDetectorService : IConsumerFunction<string, string>
    {
        public void Consume(ConsumeResult<string, string> record)
        {
            Console.WriteLine("---------------------");
            Console.WriteLine("Processando nova ordem, verificando fraude");
            Console.WriteLine($"Key: {record.Message.Key}");
            Console.WriteLine($"Value: {record.Message.Value}");
            Console.WriteLine($"Partição: {record.Partition.Value}");
            Console.WriteLine($"Offset: {record.Offset.Value}");
            Console.WriteLine($"Tópico: {record.Topic}");
            if (record.Message.Value.Contains("XYZ"))
            {
                Console.WriteLine($"FRAUDE DETECTADA");
            }
            
            try
            {
                Thread.Sleep(5000);
            }
            catch (ThreadInterruptedException e)
            {
                Console.WriteLine($"FraudDetectorService: Interrupção durante o sleep: {e.Message}");
            }

            Console.WriteLine("FraudDetectorService: Order processed (fraud check complete)");
        }

        public async Task Start(CancellationToken cancellationToken)
        {
            using (var kafkaService = new KafkaService<string, string>(
                "FraudDetectorService",
                "ECOMMERCE_NEW_ORDER",
                this))
            {
                await kafkaService.Run(cancellationToken);
            }
        }
    }
}
