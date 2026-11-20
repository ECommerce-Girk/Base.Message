namespace Base.Message;

public interface IOutboxMessageRegistry
{
    IReadOnlyDictionary<Type, string> Out { get; }
}