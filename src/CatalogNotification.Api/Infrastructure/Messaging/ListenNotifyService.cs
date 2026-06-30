using System.Text.Json;
using CatalogNotification.Api.Infrastructure.NATS;
using Microsoft.Extensions.Options;
using Npgsql;

namespace CatalogNotification.Api.Infrastructure.Messaging;

/// <summary>
/// FAST PATH: PostgreSQL trigger fires pg_notify('catalog_changes', payload)
/// -> ListenNotifyService (Npgsql Notification event / conn.WaitAsync())
/// -> looks up full payload from outbox_messages by id -> publishes to NATS
/// -> marks outbox row processed_at.
///
/// Latency ~1-2ms (one extra indexed point lookup vs. embedding payload in
/// NOTIFY). Tradeoffs:
///   - If this service's connection is down at the exact moment NOTIFY
///     fires, that signal is gone forever (Postgres does not queue NOTIFYs
///     for a disconnected listener). OutboxRelayService is the safety net —
///     it polls outbox_messages WHERE processed_at IS NULL and will catch
///     the row within its poll interval. Zero message loss.
///   - The trigger (not this service) is responsible for inserting the
///     outbox row, so every write path to service_catalogs (API, raw SQL,
///     migrations, batch jobs) gets outbox coverage, not just the API.
///
/// Runs only when CatalogPublishing:Mode is "ListenNotify" or "Both".
/// </summary>
public class ListenNotifyService : BackgroundService
{
    private readonly string _connectionString;
    private readonly INatsPublisher _natsPublisher;
    
    private readonly NatsOptions _natsOptions;
    private readonly ILogger<ListenNotifyService> _logger;

    public ListenNotifyService(
        IConfiguration configuration ,
        IOptions<NatsOptions> options,
        INatsPublisher natsPublisher,
        ILogger<ListenNotifyService> logger)
    {
        _connectionString = configuration.GetConnectionString("postgres");
        _natsOptions =  options.Value;
        _natsPublisher = natsPublisher;
        _logger = logger;
    }
    
    
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        if (_natsOptions.PublishingOptions.Mode is not (PublishMode.ListenNotify or PublishMode.Both))
        {
            _logger.LogInformation("Publishing mode not supported");
            return;
        }
        var backoff = TimeSpan.FromMilliseconds(500);
        const int maxBackoffSeconds = 30;

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                await RunListenLoopAsync(stoppingToken);

                // RunListenLoopAsync only returns normally on cancellation.
                backoff = TimeSpan.FromMilliseconds(500); // reset on clean run
            }
            catch (OperationCanceledException)
            {
                // normal shutdown
                break;
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex,
                    "ListenNotifyService connection dropped; reconnecting in {Delay}s. " +
                    "OutboxRelayService will cover any missed signals in the meantime.",
                    backoff.TotalSeconds);

                await Task.Delay(backoff, stoppingToken);
                backoff = TimeSpan.FromSeconds(Math.Min(backoff.TotalSeconds * 2, maxBackoffSeconds));
            }
        }
    }

    private async Task RunListenLoopAsync(CancellationToken ct)
    {
        // Dedicated connection, kept open for the lifetime of the loop, used
        // both for LISTEN and for the payload lookup / processed_at update
        // that happen inside HandleNotificationAsync. A single connection is
        // fine here — notification volume on this channel is low and the
        // lookup/update are both single-row, indexed operations.
        await using var conn = new NpgsqlConnection(_connectionString);
        await conn.OpenAsync(ct);
 
        conn.Notification += async (_, args) =>
        {
            try
            {
                await HandleNotificationAsync(conn, args.Payload, ct);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to handle pg_notify payload: {Payload}", args.Payload);
                // Swallow — this event fires synchronously off the listener loop;
                // the outbox path remains the safety net for this change.
            }
        };
 
        await using (var cmd = new NpgsqlCommand($"LISTEN {_natsOptions.PublishingOptions.NotifyName};", conn))
        {
            await cmd.ExecuteNonQueryAsync(ct);
        }
 
        _logger.LogInformation("ListenNotifyService listening on '{Channel}'", _natsOptions.PublishingOptions.NotifyName);
 
        while (!ct.IsCancellationRequested)
        {
            // Blocks until a notification arrives or the connection drops.
            await conn.WaitAsync(ct);
        }
    }
    
    
    private async Task HandleNotificationAsync(NpgsqlConnection conn, string payload, CancellationToken ct)
    {
        var notify = JsonSerializer.Deserialize<CatalogNotifyPayload>(
            payload, new JsonSerializerOptions { PropertyNameCaseInsensitive = true });
 
        if (notify is null)
        {
            _logger.LogWarning("Could not parse pg_notify payload: {Payload}", payload);
            return;
        }
 
        // The trigger always sets outboxMessageId now (it creates the row
        // itself), so this should always parse. The fallback hash only
        // matters for a transitional period if the old trigger is still
        // deployed somewhere, or for a write path that truly bypasses the
        // outbox table altogether.
        bool hasOutboxId = Guid.TryParse(notify.OutboxMessageId, out var msgId);
        if (!hasOutboxId)
        {
            msgId = DeterministicMsgId(notify.AccountId, notify.ChannelId, notify.Version);
            _logger.LogWarning(
                "Notification for {AccountId}/{ChannelId} v{Version} had no outboxMessageId; " +
                "using fallback id {MsgId}. CatalogPayloadJson will be unavailable on the fast path.",
                notify.AccountId, notify.ChannelId, notify.Version, msgId);
        }
 
        var subject = notify.ChangeType.Equals("Critical", StringComparison.OrdinalIgnoreCase)
            ? "catalog.critical"
            : $"catalog.{notify.AccountId}.{notify.ChannelId}";
 
        // Look up the full payload from the outbox row by id rather than
        // embedding it in the NOTIFY payload. pg_notify has an 8000-byte
        // payload limit — stuffing CatalogPayloadJson into the NOTIFY itself
        // risks the notify call failing outright for larger catalogs. This
        // is a single indexed point lookup on the primary key, sub-ms.
        string? catalogPayloadJson = hasOutboxId
            ? await TryGetOutboxPayloadAsync(conn, msgId, ct)
            : null;
 
        var evt = new CatalogChangeEvent
        {
            MessageId = msgId,
            AccountId = notify.AccountId,
            ChannelId = notify.ChannelId,
            Version = notify.Version,
            ChangeType = notify.ChangeType,
            UpdatedAt = notify.UpdatedAt,
            CatalogPayloadJson = catalogPayloadJson
        };
 
        await _natsPublisher.PublishLatestOnlyAsync(subject, evt ,  msgId.ToString() , ct);
 
        // Ack: mark processed_at only after NATS has accepted the publish
        // (PublishAsync returning successfully = NATS publish ack received).
        // This is purely an optimization so OutboxRelayService doesn't
        // redundantly re-publish a message this path already delivered — it
        // is NOT required for correctness. If this update races with the
        // relay and both publish, NATS JetStream's MsgId dedup window
        // collapses the duplicate. No downstream consumer ack is involved;
        // consumer-level delivery guarantees are JetStream's concern, not
        // the outbox's.
        if (hasOutboxId)
        {
            await MarkProcessedAsync(conn, msgId, ct);
        }
    }
 
    private async Task<string?> TryGetOutboxPayloadAsync(NpgsqlConnection conn, Guid id, CancellationToken ct)
    {
        try
        {
            await using var cmd = new NpgsqlCommand(
                "SELECT payload::text FROM outbox_messages WHERE id = @id", conn);
            cmd.Parameters.AddWithValue("id", id);
 
            var result = await cmd.ExecuteScalarAsync(ct);
            return result as string;
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex,
                "Failed to fetch outbox payload for {Id}; publishing without CatalogPayloadJson. " +
                "Client should fall back to the snapshot API.", id);
            return null;
        }
    }
 
    private async Task MarkProcessedAsync(NpgsqlConnection conn, Guid id, CancellationToken ct)
    {
        try
        {
            await using var cmd = new NpgsqlCommand(
                "UPDATE outbox_messages SET processed_at = now() " +
                "WHERE id = @id AND processed_at IS NULL", conn);
            cmd.Parameters.AddWithValue("id", id);
            await cmd.ExecuteNonQueryAsync(ct);
        }
        catch (Exception ex)
        {
            // Non-fatal: if this fails, OutboxRelayService will simply pick
            // the row up and publish again. NATS dedup handles the overlap.
            _logger.LogWarning(ex, "Failed to mark outbox row {Id} processed; relay will retry it.", id);
        }
    }
 
    /// <summary>
    /// Produces the same GUID for the same logical event regardless of which
    /// path observes it, so NATS' MsgId dedup window collapses duplicates.
    /// Fallback only — every write that goes through the current trigger
    /// gets a real outbox id, so this path should rarely if ever execute.
    /// </summary>
    private static Guid DeterministicMsgId(string accountId, string channelId, long version)
    {
        var input = $"{accountId}:{channelId}:{version}";
        var hash = System.Security.Cryptography.MD5.HashData(System.Text.Encoding.UTF8.GetBytes(input));
        return new Guid(hash);
    }
 
    private record CatalogNotifyPayload(
        string? OutboxMessageId,
        string AccountId,
        string ChannelId,
        long Version,
        string ChangeType,
        DateTimeOffset UpdatedAt);
    
    
    /// <summary>
    /// Wire format published to NATS / delivered to MQTT subscribers.
    /// MessagePack on the NATS side, JSON when bridged out over MQTT
    /// (NATS does this translation automatically for MQTT subscribers).
    /// </summary>
    public class CatalogChangeEvent
    {
        public Guid MessageId { get; set; }
        public string AccountId { get; set; } = default!;
        public string ChannelId { get; set; } = default!;
        public long Version { get; set; }
        public string ChangeType { get; set; } = default!;
        public DateTimeOffset UpdatedAt { get; set; }

        /// <summary>
        /// Only populated on the Outbox path (full payload). The ListenNotify
        /// fast path sends a slim event (no payload) — clients fetch full data
        /// via the snapshot endpoint, consistent with the "fast signal, then
        /// pull fresh data" pattern in the architecture.
        /// </summary>
        public string? CatalogPayloadJson { get; set; }
    }

    
}