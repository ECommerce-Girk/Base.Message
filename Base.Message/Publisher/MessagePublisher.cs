using System.Text;
using Base.Database;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using RabbitMQ.Client;

namespace Base.Message.Publisher;

public sealed class MessagePublisher<TDbContext>(
    IServiceScopeFactory scopeFactory,
    IOptions<BrokerSettings> brokerOptions,
    ILogger<MessagePublisher<TDbContext>> logger) : BackgroundService where TDbContext : DbContextBase
{
    private readonly BrokerSettings _settings = brokerOptions.Value;

    protected override async Task ExecuteAsync(CancellationToken ct)
    {
        await Task.Delay(TimeSpan.FromSeconds(3), ct);

        while (!ct.IsCancellationRequested)
        {
            try
            {
                using var scope = scopeFactory.CreateScope();
                var dbContext = scope.ServiceProvider.GetRequiredService<TDbContext>();

                var unsentEvents = await dbContext.OutboxMessages
                    .Where(e => e.PublishedAt == null)
                    .OrderBy(e => e.CreatedAt)
                    .Take(20)
                    .ToArrayAsync(ct);

                if (unsentEvents.Length == 0)
                {
                    await Task.Delay(5000, ct);
                    continue;
                }

                await using var connection = await Broker.Connect(_settings, _settings.ServiceName + "-outbox-publisher", ct);
                await using var channel = await Broker.OpenPublisher(connection, _settings, ct);

                foreach (var e in unsentEvents)
                {
                    await PublishNextAsync(channel, e, dbContext, ct);
                }
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                logger.LogError(ex, "Outbox message publishing error: {Message}", ex.Message);
                await Task.Delay(5000, ct);
            }
        }
    }

    private async Task PublishNextAsync(
        IChannel channel,
        OutboxMessage msg,
        DbContext dbContext,
        CancellationToken ct)
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

        msg.PublishedAt = DateTimeOffset.UtcNow;
        await dbContext.SaveChangesAsync(ct);
    }

}