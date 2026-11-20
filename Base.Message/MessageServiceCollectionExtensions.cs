
using Microsoft.Extensions.DependencyInjection;

namespace Base.Message;

public static class MessageServiceCollectionExtensions
{
    public static IServiceCollection AddMessageConsumer<TRegistry,THandler>(this IServiceCollection services) 
        where TRegistry : class, IInboxMessageRegistry
        where THandler : class, IMessageHandler
    {
        services.AddHostedService<MessageConsumer>();
        services.AddSingleton<IInboxMessageRegistry, TRegistry>();
        services.AddScoped<IMessageHandler, THandler>();
        return services;
    }

    public static IServiceCollection AddMessagePublisher<TRegistry>(this IServiceCollection services) where TRegistry : class, IOutboxMessageRegistry
    {
        services.AddHostedService<MessagePublisher>();
        services.AddSingleton<IOutboxMessageRegistry, TRegistry>();
        return services;
    }

    public static IServiceCollection AddMessageBroker<TInboxRegistry, THandler, TOutboxRegistry>(this IServiceCollection services)
        where TInboxRegistry : class, IInboxMessageRegistry
        where THandler : class, IMessageHandler
        where TOutboxRegistry : class, IOutboxMessageRegistry
    {
        services.AddMessageConsumer<TInboxRegistry, THandler>();
        services.AddMessagePublisher<TOutboxRegistry>();
        return services;
    }
}
