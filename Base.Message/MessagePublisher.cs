using System.Text;
using Base.Infra;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using RabbitMQ.Client;

namespace Base.Message;

public sealed class MessagePublisher(
    IServiceProvider serviceProvider,
    IOptions<BrokerSettings> brokerOptions,
    ILogger<MessagePublisher> logger) : BackgroundService
{
    private readonly BrokerSettings _settings = brokerOptions.Value;

    protected override async Task ExecuteAsync(CancellationToken ct)
    {
        await Task.Delay(TimeSpan.FromSeconds(3), ct);

        while (!ct.IsCancellationRequested)
        {
            try
            {
                await using var scope = serviceProvider.CreateAsyncScope();
                var outboxMessagesRepository = scope.ServiceProvider.GetRequiredService<IOutboxMessageRepository>();
                var uow = scope.ServiceProvider.GetRequiredService<IUoW>();

                var unsentMessages = await outboxMessagesRepository.GetPendingMessages(20, ct: ct);

                if (unsentMessages.Length == 0)
                {
                    await Task.Delay(5000, ct);
                    continue;
                }

                await using var connection = await Broker.Connect(_settings, _settings.ServiceName + "-outbox-publisher", ct);
                await using var channel = await Broker.OpenPublisher(connection, _settings, ct);

                foreach (var msg in unsentMessages)
                {
                    await PublishMessage(channel, msg, ct);
                    msg.PublishedAt = DateTimeOffset.UtcNow;
                    await uow.SaveChanges(ct);
                }
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                logger.LogError(ex, "Outbox message publishing error: {Message}", ex.Message);
                await Task.Delay(5000, ct);
            }
        }
    }

    private async Task PublishMessage(IChannel channel, OutboxMessage msg, CancellationToken ct)
    {
        using var timeout = CancellationTokenSource.CreateLinkedTokenSource(ct);
        timeout.CancelAfter(TimeSpan.FromSeconds(10));

        var properties = new BasicProperties
        {
            Persistent = true,
            ContentType = "application/json",
            Type = msg.Type,
            MessageId = msg.Id.ToString(),
            CorrelationId = msg.Id.ToString()
        };

        await channel.BasicPublishAsync(
            exchange: _settings.Exchange,
            routingKey: msg.Type,
            mandatory: true,
            basicProperties: properties,
            body: Encoding.UTF8.GetBytes(msg.Body),
            cancellationToken: timeout.Token);
    }

}