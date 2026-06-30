using CatalogNotification.Api.Domain.Enum;

namespace CatalogNotification.Api.Domain.Models;

public class ServiceCatalog
{
    public long Id { get; set; }
    public string AccountId { get; set; } = default!;
    public string ChannelId { get; set; } = default!;
    public long CatalogVersion { get; set; }
    public string CatalogPayloadJson { get; set; } = default!; // raw JSONB text
    public ChangeType ChangeType { get; set; }
    public DateTimeOffset UpdatedAt { get; set; }
}