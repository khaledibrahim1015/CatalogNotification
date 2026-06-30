namespace CatalogNotification.Api.Infrastructure.NATS;

// interface 




public interface INatsPublisher
{
    /// <summary>
    /// Publish  to the AllMissed stream
    /// </summary
    Task PublishAsync (string subject , object payload , string? dedupId = null , CancellationToken ct = default);

    /// <summary>
    /// Publish only to the latest-only stream Not  AllMissed Stream 
    /// </summary
    Task PublishLatestOnlyAsync (string subject , object payload , string? dedupId = null , CancellationToken ct = default);
}

public class NatsPublisher 
