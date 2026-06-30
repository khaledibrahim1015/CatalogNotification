using CatalogNotification.Api.Domain.Enum;

namespace CatalogNotification.Api.Domain.Models;

/// <summary>
/// Transactional outbox row. Written in the SAME DB transaction as the
/// ServiceCatalog update. Id doubles as the NATS MsgId for deduplication
/// across the Outbox and ListenNotify publish paths.
/// </summary>
public class OutboxMessage
{
    public Guid Id { get; set; }
    public string AccountId { get; set; } = default!;
    public string ChannelId { get; set; } = default!;
    public string Subject { get; set; } = default!;
    public ChangeType ChangeType { get; set; }
    public string PayloadJson { get; set; } = default!;
    public DateTimeOffset CreatedAt { get; set; }
    public DateTimeOffset? ProcessedAt { get; set; }
}