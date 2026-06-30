namespace CatalogNotification.Api.Domain.Models;

public class Account
{
    public string AccountId { get; set; } = default!;
    public string Name { get; set; } = default!;
    public DateTimeOffset CreatedAt { get; set; }
    
}