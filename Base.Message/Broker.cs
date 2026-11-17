using Microsoft.Extensions.Logging;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace Base.Message;

internal static class Broker
{
    internal static Task<IConnection> Connect(BrokerSettings settings, string clientProvidedName, CancellationToken ct) =>
        new ConnectionFactory
        {
            HostName = settings.Host,
            UserName = settings.User,
            Password = settings.Password,
            ClientProvidedName = $"{settings.ServiceName}-{clientProvidedName}",
            AutomaticRecoveryEnabled = false, // Lo manejamos de forma controlada con el BackgroundService
            RequestedConnectionTimeout = TimeSpan.FromSeconds(5),
            RequestedHeartbeat = TimeSpan.FromSeconds(10)
        }.CreateConnectionAsync(ct);

    internal static async Task<IChannel> OpenConsumer(
        IConnection connection,
        BrokerSettings settings,
        string queueName,
        string deadLetterQueueName,
        string[] routingKeys,
        CancellationToken ct)
    {
        var channelOptions = new CreateChannelOptions(publisherConfirmationsEnabled: true, publisherConfirmationTrackingEnabled: true);
        var channel = await connection.CreateChannelAsync(channelOptions, ct);

        await channel.ExchangeDeclareAsync(settings.Exchange, ExchangeType.Topic, durable: true, autoDelete: false, cancellationToken: ct);

        var args = new Dictionary<string, object?> {
            ["x-queue-type"] = "quorum", // default, highly available queue type designed for data safety and fault tolerance.
            ["x-delivery-limit"] = -1    // Disable the delivery limit. We handle retries in the consumer code, not the broker.
        };

        await channel.QueueDeclareAsync(queueName, durable: true, exclusive: false, autoDelete: false, args, cancellationToken: ct);
        await channel.QueueDeclareAsync(deadLetterQueueName, durable: true, exclusive: false, autoDelete: false, args, cancellationToken: ct);
        foreach (var routingKey in routingKeys)
        {
            await channel.QueueBindAsync(queueName, settings.Exchange, routingKey, cancellationToken: ct);
        }

        await channel.BasicQosAsync(
            prefetchSize: 0,  // No message limit
            prefetchCount: 1, // One message at a time.
            global: false,
            ct);

        return channel;
    }

    internal static async Task<IChannel> OpenPublisher(
        IConnection connection,
        BrokerSettings settings,
        CancellationToken ct)
    {
        var channelOptions = new CreateChannelOptions(publisherConfirmationsEnabled: true, publisherConfirmationTrackingEnabled: true);
        var channel = await connection.CreateChannelAsync(channelOptions, ct);

        await channel.ExchangeDeclareAsync(settings.Exchange, ExchangeType.Topic, durable: true, autoDelete: false, cancellationToken: ct);

        await channel.BasicQosAsync(
            prefetchSize: 0,  // No message limit
            prefetchCount: 1, // One message at a time.
            global: false,
            ct);

        return channel;
    }

    internal static async Task Quarantine(
        string exchange,
        string deadLetterQueueName,
        IChannel channel,
        BasicDeliverEventArgs delivery,
        string reason,
        ILogger logger,
        CancellationToken ct)
    {
        var properties = new BasicProperties(delivery.BasicProperties)
        {
            Persistent = true,
            Headers = new Dictionary<string, object?> { ["failure-reason"] = reason }
        };

        using var timeout = CancellationTokenSource.CreateLinkedTokenSource(ct);
        timeout.CancelAfter(TimeSpan.FromSeconds(10));

        // Ensure DLQ copy is saved BEFORE ACK. A crash may cause a duplicate, but guarantees no data loss.
        await channel.BasicPublishAsync(exchange, deadLetterQueueName, mandatory: true, properties, delivery.Body, timeout.Token);
        await channel.BasicAckAsync(delivery.DeliveryTag, multiple: false, ct);

        logger.LogWarning("Event {EventId} moved to {Queue}: {Reason}", delivery.BasicProperties.MessageId, deadLetterQueueName, reason);
    }
}