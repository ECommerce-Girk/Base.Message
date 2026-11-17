using System.Text.Json;
using Base.Infra;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace Base.Message.Consumer;

public sealed class MessageConsumer(
    IServiceProvider serviceProvider,
    IOptions<BrokerSettings> brokerOptions,
    IInboxMessageRegistry inboxMessageRegistry,
    ILogger<MessageConsumer> logger) : BackgroundService
{
    private readonly BrokerSettings _settings = brokerOptions.Value;
    private readonly string _queueName = brokerOptions.Value.ServiceName + ".main-queue";
    private readonly string _deadLetterQueueName = brokerOptions.Value.ServiceName + ".dead-letter-queue";

    protected override async Task ExecuteAsync(CancellationToken ct)
    {
        while (!ct.IsCancellationRequested)
        {
            try
            {
                await using var connection = await Broker.Connect(_settings, _settings.ServiceName + "-message-consumer", ct);
                await using var channel = await Broker.OpenConsumer(connection, _settings, _queueName, _deadLetterQueueName, inboxMessageRegistry.In.Keys.ToArray(), ct);
        
                var ended = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
                connection.ConnectionShutdownAsync += (_, _) => { ended.TrySetResult(); return Task.CompletedTask; };
                channel.ChannelShutdownAsync += (_, _) => { ended.TrySetResult(); return Task.CompletedTask; };

                var consumer = new AsyncEventingBasicConsumer(channel);
                consumer.ReceivedAsync += async (_, delivery) =>
                {
                    try 
                    {
                        await Handle(channel, delivery, ct);
                    }
                    catch (OperationCanceledException) when (ct.IsCancellationRequested)
                    {
                        ended.TrySetResult();
                    }
                    catch (Exception exception)
                    {
                        // If ACK or Broker.Quarantine publish fails, reconnect. The original is still unacked.
                        logger.LogWarning(exception, "Delivery interrupted. Reconnecting so the broker can redeliver.");
                        ended.TrySetResult();
                    }
                };

                await channel.BasicConsumeAsync(_queueName, autoAck: false, consumer, ct);
                await ended.Task.WaitAsync(ct);
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested) 
            {
                break; 
            }
            catch (Exception exception) 
            {
                logger.LogWarning(exception, "Consumer disconnected; retrying in 2s.");
            }

            try 
            { 
                await Task.Delay(TimeSpan.FromSeconds(2), ct); 
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
                break;
            }
        }
    }

    private async Task Handle(IChannel channel, BasicDeliverEventArgs delivery, CancellationToken ct)
    {
        var eventTypeString = delivery.BasicProperties.Type;
        if (string.IsNullOrWhiteSpace(eventTypeString))
        {
            await Broker.Quarantine(_settings.Exchange, _deadLetterQueueName, channel, delivery, "Missing event type in properties.", logger, ct);
            return;
        }
        
        if(!Guid.TryParse(delivery.BasicProperties.MessageId, out var messageId))
        {
            await Broker.Quarantine(_settings.Exchange, _deadLetterQueueName, channel, delivery, "Missing MessageId in properties.", logger, ct);
            return;
        }

        if (!inboxMessageRegistry.In.TryGetValue(eventTypeString, out var messageType))
        {
            await Broker.Quarantine(_settings.Exchange, _deadLetterQueueName, channel, delivery, $"Unknown event type: {eventTypeString}.", logger, ct);
            return;
        }

        var message = JsonSerializer.Deserialize(delivery.Body.Span, messageType) as IDomainEvent;

        if (message is null)
        {
            await Broker.Quarantine(_settings.Exchange, _deadLetterQueueName, channel, delivery, $"Failed to deserialize message of type: {eventTypeString}.", logger, ct);
            return;
        }

        var inboxMessage = InboxMessage.From(message, messageId, eventTypeString);
        
        for (var attempt = 1; attempt <= 5; attempt++)
        {
            try
            {
                await using var scope = serviceProvider.CreateAsyncScope();
                var handler = inboxMessageRegistry.TryGetHandler(message, scope.ServiceProvider);
                if(handler is null)
                {
                    await Broker.Quarantine(_settings.Exchange, _deadLetterQueueName, channel, delivery, $"No handler registered for: {eventTypeString}.", logger, ct);
                    return;
                }

                var processor = scope.ServiceProvider.GetRequiredService<IMessageProcessor>();
                await processor.Process(scope, message, handler, inboxMessage, ct);
                await channel.BasicAckAsync(delivery.DeliveryTag, false, ct);
                return;
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
                throw;
            }
            catch (InvalidMessageException exception)
            {
                await Broker.Quarantine(_settings.Exchange, _deadLetterQueueName, channel, delivery, exception.Message, logger, ct);
                return;
            }
            catch (Exception exception) when (attempt < 5)
            {
                logger.LogWarning(exception,
                    "Event {EventId}: attempt {Attempt}/5 failed.",
                    messageId, attempt);

                await Task.Delay(TimeSpan.FromSeconds(Math.Pow(2, attempt - 1)), ct);
            }
            catch (Exception exception)
            {
                await Broker.Quarantine(_settings.Exchange, _deadLetterQueueName, channel, delivery, $"Five attempts failed: {exception.GetType().Name}", logger, ct);
            }
        }
    }
}