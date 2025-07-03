using Confluent.Kafka;
using static Confluent.Kafka.ConfigPropertyNames;

namespace Consumer_Kafka
{
    public class FraudDetectorService
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
                var topic = "ECOMMERCE_NEW_ORDER";
                consumer.Subscribe(topic);
                Console.WriteLine($"Consumidor inscrito no tópico: {topic}");

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
                            Console.WriteLine("Processando nova ordem, verificando fraude");
                            Console.WriteLine($"Key: {consumeResult.Message.Key}");
                            Console.WriteLine($"Value: {consumeResult.Message.Value}");
                            Console.WriteLine($"Partição: {consumeResult.Partition.Value}");
                            Console.WriteLine($"Offset: {consumeResult.Offset.Value}");
                            Console.WriteLine($"Tópico: {consumeResult.Topic}");
                            if (consumeResult.Message.Value.Contains("XYZ"))
                            {
                                Console.WriteLine($"FRAUDE DETECTADA");
                            }

                            Thread.Sleep(7000);

                            Console.WriteLine("Ordem processada");

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
                GroupId = "FraudDetectorService",
                AutoOffsetReset = AutoOffsetReset.Earliest,
                EnableAutoCommit = false
            };
        }
    }
}
