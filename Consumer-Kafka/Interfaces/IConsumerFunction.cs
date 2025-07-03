using Confluent.Kafka;

namespace Consumer_Kafka.Interfaces
{
    public interface IConsumerFunction<TKey, TValue>
    {
        void Consume(ConsumeResult<TKey, TValue> record);
    }
}
