namespace Consumer_Kafka.Models
{
    public class Order
    {
        public string UserId { get; set; }
        public string OrderId { get; set; }
        public decimal Amount { get; set; }

        public Order(string userId, string orderId, decimal amount)
        {
            UserId = userId;
            OrderId = orderId;
            Amount = amount;
        }

        public Order() { }

        public override string ToString()
        {
            return $"Order {{ UserId='{UserId}', OrderId='{OrderId}', Amount={Amount} }}";
        }
    }
}
