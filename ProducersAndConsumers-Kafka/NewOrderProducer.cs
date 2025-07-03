using Confluent.Kafka;

namespace ProducersAndConsumers_Kafka;


public class NewOrderProducer
{
    public async Task ProduceNewOrder()
    {
        using (var dispatcher = new KafkaDispatcher<string, string>())
        {
            var new_order_topic = "ECOMMERCE_NEW_ORDER";
            var send_email_topic = "ECOMMERCE_SEND_EMAIL";

            var orderId = Guid.NewGuid().ToString();
            var orderValue = $"{orderId},67523,1234";
            var emailValue = $"Thank you for your order {orderId}! We are processing your order!";
            await dispatcher.SendAsync(new_order_topic, orderId, orderValue);

            await dispatcher.SendAsync(send_email_topic, orderId, emailValue);

            Console.WriteLine("Todas as mensagens foram enviadas ou estão em processo de envio.");
        }
    }

    public async Task ProduceMultipleNewOrders(int numberOfOrders)
    {
        using (var dispatcher = new KafkaDispatcher<string, string>())
        {
            var new_order_topic = "ECOMMERCE_NEW_ORDER";
            var send_email_topic = "ECOMMERCE_SEND_EMAIL";

            for (int i = 0; i < numberOfOrders; i++)
            {
                var orderId = Guid.NewGuid().ToString();
                var orderValue = $"{orderId},67523,1234_{i}";
                var emailValue = $"Thank you for your order {orderId}! We are processing your order!";

                Console.WriteLine($"Produzindo mensagem {i + 1}/{numberOfOrders} para a ordem {orderId}...");
                
                await dispatcher.SendAsync(new_order_topic, orderId, orderValue);
                await dispatcher.SendAsync(send_email_topic, orderId, emailValue);

                await Task.Delay(50);
            }

            Console.WriteLine($"Concluído o envio de {numberOfOrders} novas ordens.");
        }
    }
}
