namespace CatalogNotification.Api.Domain.Models;

public class PosChannel
{
    public string ChannelId { get; set; } = default!;
    public string AccountId { get; set; } = default!;
    public string Name { get; set; } = default!;
    public bool IsActive { get; set; }
    public DateTimeOffset CreatedAt { get; set; }
}