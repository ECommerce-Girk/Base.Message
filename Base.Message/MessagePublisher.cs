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
    IOutboxMessageRegistry outboxMessageRegistry,
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

                var pendingMessages = await outboxMessagesRepository.ClaimPendingMessages(20, ct);

                if (pendingMessages.Length == 0)
                {
                    await Task.Delay(5000, ct);
                    continue;
                }

                await using var connection = await Broker.Connect(_settings, "outbox-publisher", ct);
                await using var channel = await Broker.OpenPublisher(connection, _settings, ct);

                foreach (var msg in pendingMessages)
                {
                    var messageType = GetMessageType(msg.ClrType);
                    await PublishMessage(channel, messageType, msg.Id, msg.Body, ct);
                    msg.PublishedAt = DateTimeOffset.UtcNow;
                    msg.MessageType = messageType;
                }

                await uow.SaveChanges(ct);
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                logger.LogError(ex, "Outbox message publishing error: {Message}", ex.Message);
                await Task.Delay(5000, ct);
            }
        }
    }

    private string GetMessageType(string clrType)
    {
         return outboxMessageRegistry.Out.FirstOrDefault(x => x.Value.FullName == clrType).Key ?? throw new InvalidOperationException(
            $"No message type registered for CLR type: {clrType}");
    }

    private async Task PublishMessage(IChannel channel, string messageType, Guid messageId, string messageBody, CancellationToken ct)
    {
        using var timeout = CancellationTokenSource.CreateLinkedTokenSource(ct);
        timeout.CancelAfter(TimeSpan.FromSeconds(10));

        var properties = new BasicProperties
        {
            Persistent = true,
            ContentType = "application/json",
            Type = messageType,
            MessageId = messageId.ToString(),
            CorrelationId = messageId.ToString()
        };

        await channel.BasicPublishAsync(
            exchange: _settings.Exchange,
            routingKey: messageType,
            mandatory: true,
            basicProperties: properties,
            body: Encoding.UTF8.GetBytes(messageBody),
            cancellationToken: timeout.Token);
    }

}