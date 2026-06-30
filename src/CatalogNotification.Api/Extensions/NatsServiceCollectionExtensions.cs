using CatalogNotification.Api.Infrastructure.Messaging;
using CatalogNotification.Api.Infrastructure.NATS;
using Microsoft.Extensions.Options;

namespace CatalogNotification.Api.Extensions;

public static class NatsServiceCollectionExtensions
{
    /// <summary>
    /// Registers NatsOptions (bound from the "NATS" config section) plus the
    /// connection provider, stream setup, and publisher. ValidateOnStart() means
    /// a misconfigured NATS section (missing Urls, missing/wrong credentials)
    /// fails application startup immediately instead of on first use.
    ///
    /// Call from Program.cs:
    ///   builder.Services.AddNatsCatalogServices(builder.Configuration);
    /// </summary>
    public static IServiceCollection AddNatsCatalogServices(
        this IServiceCollection services,
        IConfiguration configuration)
    {
        services
            .AddOptions<NatsOptions>()
            .Bind(configuration.GetSection(NatsOptions.SectionName))
            .ValidateOnStart();
 
        services.AddSingleton<IValidateOptions<NatsOptions>, NatsOptionsValidator>();
 
        services.AddSingleton<INatsConnectionProvider, NatsConnectionProvider>();
        services.AddSingleton<INatsStreamSetup, NatsStreamSetup>();
        services.AddSingleton<INatsPublisher, NatsPublisher>();
        // ----------------------------------------------------------------------------
        // Both publish-path background services. Each checks CatalogPublishing:Mode
        // internally and no-ops if it's not the selected (or "Both") mode — this is
        // the "implement both, choose at runtime via config" approach.
        // ----------------------------------------------------------------------------
        // services.AddHostedService<OutboxRelayService>();
        services.AddHostedService<ListenNotifyService>();
        return services;
    }
}

