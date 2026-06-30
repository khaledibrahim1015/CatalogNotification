using CatalogNotification.Api.Domain.Models;
using Microsoft.EntityFrameworkCore;

namespace CatalogNotification.Api.Data;

public class CatalogDbContext : DbContext
{
    public CatalogDbContext(DbContextOptions<CatalogDbContext> options) : base(options) {}

    public DbSet<Account>  Accounts  => Set<Account>();
    public DbSet<PosChannel> PosChannels => Set<PosChannel>();
    public DbSet<ServiceCatalog> ServiceCatalogs => Set<ServiceCatalog>();
    
    
    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        base.OnModelCreating(modelBuilder);

        // --- Account Configuration ---
        modelBuilder.Entity<Account>(e =>
        {
            e.ToTable("accounts");
            e.HasKey(x => x.AccountId);
            e.Property(x => x.AccountId).HasColumnName("account_id");
            e.Property(x => x.Name).HasColumnName("name");
            e.Property(x => x.CreatedAt).HasColumnName("created_at");
        });


        // --- PosChannel Configuration ---
        modelBuilder.Entity<PosChannel>(entity =>
        {
            entity.ToTable("pos_channels");
            entity.HasKey(x => new { x.AccountId, x.ChannelId });
            entity.Property(x => x.AccountId).HasColumnName("account_id");
            entity.Property(x => x.ChannelId).HasColumnName("channel_id");
            entity.Property(x => x.Name).HasColumnName("name");
            entity.Property(x => x.IsActive).HasColumnName("is_active");
            entity.Property(x => x.CreatedAt).HasColumnName("created_at");
            
            // Foreign Key relationship to Account
            entity.HasOne<Account>()
                .WithMany()
                .HasForeignKey(e => e.AccountId)
                .OnDelete(DeleteBehavior.Cascade);
            
        });

        // --- ServiceCatalog Configuration ---
        modelBuilder.Entity<ServiceCatalog>(entity =>
        {
            entity.ToTable("service_catalogs");
            entity.HasKey(e => e.Id);
            
            entity.Property(x => x.Id).HasColumnName("id");
            entity.Property(x => x.AccountId).HasColumnName("account_id");
            entity.Property(x => x.ChannelId).HasColumnName("channel_id");
            entity.Property(x => x.CatalogVersion).HasColumnName("catalog_version");
            entity.Property(x => x.CatalogPayloadJson).HasColumnName("catalog_payload").HasColumnType("jsonb");
            entity.Property(x => x.ChangeType)
                .HasColumnName("change_type")
                .HasConversion<string>();
            entity.Property(x => x.UpdatedAt).HasColumnName("updated_at");

            // Foreign Key relationships
            entity.HasOne<Account>()
                .WithMany()
                .HasForeignKey(e => e.AccountId)
                .OnDelete(DeleteBehavior.Cascade);

            entity.HasOne<PosChannel>()
                .WithMany()
                .HasForeignKey(e => new { e.AccountId, e.ChannelId })
                .OnDelete(DeleteBehavior.Cascade);
                  
            // Optional composite index for fast version lookups per channel
            entity.HasIndex(x => new { x.AccountId, x.ChannelId }).IsUnique();

        });
        
        modelBuilder.Entity<OutboxMessage>(entity =>
        {
            entity.ToTable("outbox_messages");
            entity.HasKey(x => x.Id);
            entity.Property(x => x.Id).HasColumnName("id");
            entity.Property(x => x.AccountId).HasColumnName("account_id");
            entity.Property(x => x.ChannelId).HasColumnName("channel_id");
            entity.Property(x => x.Subject).HasColumnName("subject");
            entity.Property(x => x.ChangeType)
                .HasColumnName("change_type")
                .HasConversion<string>();
            entity.Property(x => x.PayloadJson).HasColumnName("payload").HasColumnType("jsonb");
            entity.Property(x => x.CreatedAt).HasColumnName("created_at");
            entity.Property(x => x.ProcessedAt).HasColumnName("processed_at");
        });
        
        
        
        
        
        
        
    }
    
}