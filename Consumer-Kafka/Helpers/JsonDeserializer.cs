using Confluent.Kafka;
using System.Text;
using System.Text.Json;

namespace Consumer_Kafka.Helpers
{
    public class JsonDeserializer<T> : IDeserializer<T>
    {
        public T Deserialize(ReadOnlySpan<byte> data, bool isNull, SerializationContext context)
        {
            if (isNull)
            {
                return default;
            }
            return JsonSerializer.Deserialize<T>(Encoding.UTF8.GetString(data));
        }
    }
}
