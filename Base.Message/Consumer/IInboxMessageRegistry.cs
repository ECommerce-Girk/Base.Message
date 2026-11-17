namespace Base.Message.Consumer;

public interface IInboxMessageRegistry
{
    IReadOnlyDictionary<string, Type> In { get; }
    IMessageHandler<IDomainEvent>? TryGetHandler(IDomainEvent domainEvent, IServiceProvider serviceProvider);
}