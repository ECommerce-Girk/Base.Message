using Base.Infra;

namespace Base.Message;

public interface IMessageHandler
{
    Task Handle(IServiceProvider serviceProvider, IDomainEvent message, InboxMessage inboxMessage, CancellationToken ct);
}

public class InvalidMessageException(string message) : Exception(message);
