namespace Base.Message.Consumer;

public interface IMessageHandler<in T> where T : IDomainEvent
{
    Task Handle(T message, CancellationToken ct);
}