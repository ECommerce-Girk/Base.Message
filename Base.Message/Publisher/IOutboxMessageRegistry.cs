namespace Base.Message.Publisher;

public interface IOutboxMessageRegistry
{
    IReadOnlyDictionary<string, Type> Out { get; }
}