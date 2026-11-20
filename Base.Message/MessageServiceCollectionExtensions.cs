
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;

namespace Base.Message;

public static class MessageServiceCollectionExtensions
{
    public static IServiceCollection AddMessageConsumer<TRegistry,THandler>(this IServiceCollection services, IConfiguration configuration) 
        where TRegistry : class, IInboxMessageRegistry
        where THandler : class, IMessageHandler
    {
        services.Configure<BrokerSettings>(configuration.GetSection("BrokerSettings"));
        services.AddHostedService<MessageConsumer>();
        services.AddSingleton<IInboxMessageRegistry, TRegistry>();
        services.AddScoped<IMessageHandler, THandler>();
        return services;
    }

    public static IServiceCollection AddMessagePublisher<TRegistry>(this IServiceCollection services, IConfiguration configuration)
        where TRegistry : class, IOutboxMessageRegistry
    {
        services.Configure<BrokerSettings>(configuration.GetSection("BrokerSettings"));
        services.AddHostedService<MessagePublisher>();
        services.AddSingleton<IOutboxMessageRegistry, TRegistry>();
        return services;
    }

    public static IServiceCollection AddMessageBroker<TInboxRegistry, THandler, TOutboxRegistry>(this IServiceCollection services, IConfiguration configuration)
        where TInboxRegistry : class, IInboxMessageRegistry
        where THandler : class, IMessageHandler
        where TOutboxRegistry : class, IOutboxMessageRegistry
    {
        services.AddMessageConsumer<TInboxRegistry, THandler>(configuration);
        services.AddMessagePublisher<TOutboxRegistry>(configuration);
        return services;
    }
}
