using CatalogNotification.Api.Domain.Enum;
using CatalogNotification.Api.Domain.Models;
using Microsoft.EntityFrameworkCore;

namespace CatalogNotification.Api.Data;

public static class DbInitializer
{
    public static async Task SeedDataAsync(this CatalogDbContext context)
    {
        // 1. Ensure the database matches the latest migrations
        await context.Database.MigrateAsync();

        // 2. Check if data already exists to avoid duplicate seeding
        if (await context.Accounts.AnyAsync()) return;

        // 3. Define and save seed data
        var accounts = new List<Account>
        {
            new() { AccountId = "acc-fintech-01", Name = "Northbridge Payments Inc.", CreatedAt = DateTimeOffset.UtcNow },
            new() { AccountId = "acc-fintech-02", Name = "Meridian Lending Group", CreatedAt = DateTimeOffset.UtcNow }
        };

        var channels = new List<PosChannel>
        {
            // Northbridge Payments — different product lines/channels
            new() { AccountId = "acc-fintech-01", ChannelId = "card-issuing",   Name = "Card Issuing API",      IsActive = true, CreatedAt = DateTimeOffset.UtcNow },
            new() { AccountId = "acc-fintech-01", ChannelId = "wire-transfer",  Name = "Wire Transfer Gateway", IsActive = true, CreatedAt = DateTimeOffset.UtcNow },
            new() { AccountId = "acc-fintech-01", ChannelId = "fx-conversion",  Name = "FX Conversion Service", IsActive = true, CreatedAt = DateTimeOffset.UtcNow },

            // Meridian Lending — loan products
            new() { AccountId = "acc-fintech-02", ChannelId = "personal-loans", Name = "Personal Loans Desk",   IsActive = true, CreatedAt = DateTimeOffset.UtcNow },
            new() { AccountId = "acc-fintech-02", ChannelId = "merchant-cash",  Name = "Merchant Cash Advance", IsActive = false, CreatedAt = DateTimeOffset.UtcNow }
        };

        var catalogs = new List<ServiceCatalog>
        {
            new()
            {
                AccountId = "acc-fintech-01",
                ChannelId = "card-issuing",
                CatalogVersion = 1,
                CatalogPayloadJson = "{\"products\": [" +
                    "{\"sku\": \"CARD-VIRTUAL-USD\", \"name\": \"Virtual Card (USD)\", \"feeBps\": 25, \"currency\": \"USD\"}," +
                    "{\"sku\": \"CARD-PHYSICAL-USD\", \"name\": \"Physical Debit Card (USD)\", \"feeBps\": 15, \"currency\": \"USD\"}" +
                    "]}",
                ChangeType = ChangeType.Update,
                UpdatedAt = DateTimeOffset.UtcNow
            },
            new()
            {
                AccountId = "acc-fintech-01",
                ChannelId = "wire-transfer",
                CatalogVersion = 1,
                CatalogPayloadJson = "{\"products\": [" +
                    "{\"sku\": \"WIRE-DOMESTIC\", \"name\": \"Domestic Wire\", \"flatFee\": 15.00, \"currency\": \"USD\"}," +
                    "{\"sku\": \"WIRE-INTL\", \"name\": \"International Wire\", \"flatFee\": 45.00, \"currency\": \"USD\"}" +
                    "]}",
                ChangeType = ChangeType.Critical,
                UpdatedAt = DateTimeOffset.UtcNow
            },
            new()
            {
                AccountId = "acc-fintech-01",
                ChannelId = "fx-conversion",
                CatalogVersion = 1,
                CatalogPayloadJson = "{\"pairs\": [" +
                    "{\"pair\": \"USD/EUR\", \"spreadBps\": 35}," +
                    "{\"pair\": \"USD/GBP\", \"spreadBps\": 40}" +
                    "]}",
                ChangeType = ChangeType.Update,
                UpdatedAt = DateTimeOffset.UtcNow
            },
            new()
            {
                AccountId = "acc-fintech-02",
                ChannelId = "personal-loans",
                CatalogVersion = 1,
                CatalogPayloadJson = "{\"products\": [" +
                    "{\"sku\": \"LOAN-PERSONAL-3YR\", \"name\": \"3-Year Personal Loan\", \"aprPct\": 9.99, \"maxAmount\": 25000}," +
                    "{\"sku\": \"LOAN-PERSONAL-5YR\", \"name\": \"5-Year Personal Loan\", \"aprPct\": 11.49, \"maxAmount\": 50000}" +
                    "]}",
                ChangeType = ChangeType.Update,
                UpdatedAt = DateTimeOffset.UtcNow
            }
        };

        await context.Accounts.AddRangeAsync(accounts);
        await context.PosChannels.AddRangeAsync(channels);
        await context.ServiceCatalogs.AddRangeAsync(catalogs);

        await context.SaveChangesAsync();
    }
}