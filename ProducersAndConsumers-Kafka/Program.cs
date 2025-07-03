using ProducersAndConsumers_Kafka;

NewOrderProducer newOrderProducer = new NewOrderProducer();
await newOrderProducer.ProduceNewOrder();