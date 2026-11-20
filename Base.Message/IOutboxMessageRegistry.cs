namespace Base.Message;

public interface IOutboxMessageRegistry
{
    IReadOnlyDictionary<string, Type> Out { get; }
}