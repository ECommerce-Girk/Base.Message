namespace Base.Message;

public interface IInboxMessageRegistry
{
    IReadOnlyDictionary<string, Type> In { get; }
}