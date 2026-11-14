namespace Base.Message;

public record BrokerSettings
{
    public required string ServiceName { get; set; } // "identity", "sales"
    public required string Host { get; set; }
    public required string User { get; set; }
    public required string Password { get; set; }
    public string Exchange => "ecommerce";
}