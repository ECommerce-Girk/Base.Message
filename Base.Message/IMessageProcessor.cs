using Base.Infra;
using Microsoft.Extensions.DependencyInjection;

namespace Base.Message;

public interface IMessageProcessor
{
    Task Process(
        AsyncServiceScope scope,
        IDomainEvent message,
        IMessageHandler<IDomainEvent> handler,
        InboxMessage inboxMessage,
        CancellationToken ct);
}

public class InvalidMessageException(string message) : Exception(message);
