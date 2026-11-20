
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

    public static IServiceCollection AddMessageBroker<TMessageRegistry, THandler>(this IServiceCollection services, IConfiguration configuration)
        where TMessageRegistry : class, IInboxMessageRegistry, IOutboxMessageRegistry
        where THandler : class, IMessageHandler
    {
        services.AddMessageConsumer<TMessageRegistry, THandler>(configuration);
        services.AddMessagePublisher<TMessageRegistry>(configuration);
        return services;
    }
}
