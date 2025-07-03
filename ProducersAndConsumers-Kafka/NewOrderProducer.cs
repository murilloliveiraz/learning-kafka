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
            var orderValue = "teste,teste,teste";
            var emailValue = "TESTE: Thank you for your order! We are processing your order!";
            await dispatcher.SendAsync(new_order_topic, orderValue, orderValue);

            await dispatcher.SendAsync(send_email_topic, emailValue, emailValue);

            Console.WriteLine("Todas as mensagens foram enviadas ou estão em processo de envio.");
        }
    }
}
