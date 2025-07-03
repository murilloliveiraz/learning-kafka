using Confluent.Kafka;
using System.Text.RegularExpressions;
using static Confluent.Kafka.ConfigPropertyNames;

namespace Consumer_Kafka
{
    public class LogService
    {
        public async Task ConsumeNewOrders(CancellationToken cancellationToken)
        {
            var config = GetConsumerConfig();

            using (var consumer = new ConsumerBuilder<string, string>(config)
                .SetKeyDeserializer(Deserializers.Utf8)
                .SetValueDeserializer(Deserializers.Utf8)
                .SetErrorHandler((_, e) => Console.WriteLine($"Erro interno do Kafka: {e.Reason}"))
                .SetStatisticsHandler((_, json) => Console.WriteLine($"Estatísticas Kafka: {json}"))
                .Build())
            {
                var topics = new List<string> { "ECOMMERCE_NEW_ORDER", "ECOMMERCE_SEND_EMAIL" };
                consumer.Subscribe(topics);
                Console.WriteLine($"LogService inscrito nos tópicos:");
                foreach (var topic in topics)
                {
                    Console.WriteLine(topic);
                }

                try
                {
                    while (!cancellationToken.IsCancellationRequested)
                    {
                        try
                        {
                            var consumeResult = consumer.Consume(cancellationToken);

                            if (consumeResult == null)
                            {
                                continue;
                            }

                            Console.WriteLine("---------------------");
                            Console.WriteLine($"LOG DE EVENTO DO KAFKA:");
                            Console.WriteLine($"  Tópico: {consumeResult.Topic}");
                            Console.WriteLine($"  Key: {consumeResult.Message.Key}");
                            Console.WriteLine($"  Value: {consumeResult.Message.Value}");
                            Console.WriteLine($"  Partição: {consumeResult.Partition.Value}");
                            Console.WriteLine($"  Offset: {consumeResult.Offset.Value}");
                            Console.WriteLine($"  Timestamp: {consumeResult.Timestamp.UtcDateTime}");

                            consumer.Commit(consumeResult);
                        }
                        catch (ConsumeException ex)
                        {
                            Console.WriteLine($"Erro ao consumir mensagem: {ex.Error.Reason}");
                        }
                        catch (OperationCanceledException)
                        {
                            Console.WriteLine("Consumo cancelado.");
                            break;
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine($"Erro inesperado durante o processamento: {ex.Message}");
                        }
                    }
                }
                finally
                {
                    Console.WriteLine("Fechando consumidor...");
                    consumer.Close();
                }
            }
        }

        private static ConsumerConfig GetConsumerConfig()
        {
            return new ConsumerConfig
            {
                BootstrapServers = "127.0.0.1:9092",
                GroupId = "LogService",
                AutoOffsetReset = AutoOffsetReset.Earliest,
                EnableAutoCommit = false
            };
        }
    }
}
