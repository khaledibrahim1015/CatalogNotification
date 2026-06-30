using Microsoft.Extensions.Options;

namespace CatalogNotification.Api.Infrastructure.NATS;


public class NatsOptions
{
    public const string SectionName = "NATS";
 
    /// <summary>
    /// Seed server URLs for the 3-node catalog-cluster. Configure as a JSON array in
    /// appsettings — see example below. The client randomly picks one to connect to
    /// and automatically fails over to another if that node goes down (NATS.Net's
    /// built-in cluster awareness — see NatsConnectionProvider for how this is joined).
    ///
    /// appsettings.json:
    /// {
    ///   "NATS": {
    ///     "Urls": [
    ///       "nats://nats-1:4222",
    ///       "nats://nats-2:4222",
    ///       "nats://nats-3:4222"
    ///     ]
    ///   }
    /// }
    ///
    /// Or via environment variables (container-friendly, e.g. docker-compose):
    ///   NATS__Urls__0=nats://nats-1:4222
    ///   NATS__Urls__1=nats://nats-2:4222
    ///   NATS__Urls__2=nats://nats-3:4222
    /// </summary>
    public string[] Urls { get; set; } = ["nats://localhost:4222"];
 
    public string ClusterName { get; set; } = "catalog-cluster";
 
    /// <summary>APP account username (NATS_APP_USER in infra .env). Never SYS.</summary>
    public string? Username { get; set; }
 
    /// <summary>APP account password (NATS_APP_PASSWORD in infra .env). Never SYS.</summary>
    public string? Password { get; set; }
 
    /// <summary>Name of the JetStream stream backing catalog notifications. Must match
    /// the stream created by infra/nats/create-stream.sh (CATALOG_STREAM by default).</summary>
    public string StreamName { get; set; } = "CATALOG_STREAM";
 
    public string SubjectPrefix { get; set; } = "catalog";
    public int DedupWindowMinutes { get; set; } = 5;
 
    /// <summary>
    /// Retention/replay semantics for reconnecting mobile clients — drives the
    /// MaxMsgsPerSubject/MaxAge settings NatsStreamSetup uses when creating or
    /// updating the stream. Mirrors the POLICY argument to create-stream.sh.
    ///
    /// appsettings.json: "MissedMessagePolicy": "LastOnly" | "AllMissed" | "TimeBounded"
    /// </summary>
    public MissedMessagePolicy MissedMessagePolicy { get; set; } = MissedMessagePolicy.LastOnly;
 
    /// <summary>
    /// Required when MissedMessagePolicy is TimeBounded — the retention window
    /// (equivalent to the MAX_AGE arg passed to create-stream.sh, e.g. "72h").
    /// Configure in appsettings as a timespan string, e.g. "3.00:00:00" for 72h,
    /// or via env var NATS__TimeBoundedMaxAge=3.00:00:00.
    /// </summary>
    public TimeSpan? TimeBoundedMaxAge { get; set; }

    public PublishingOptions PublishingOptions { get; set; } = new();

    



}

public class PublishingOptions
{
    public PublishMode Mode { get; set; } = PublishMode.Both;
    public int OutboxPollIntervalMs { get; set; } = 500;
    public string NotifyName  { get; set; } = "catalog_changes";
}

/// <summary>
/// Selects which publish path(s) push catalog changes into NATS JetStream.
///
///   Outbox       -> only OutboxRelayService runs (poll every 500ms, guaranteed delivery,
///                   higher latency: 0-500ms)
///   ListenNotify -> only ListenNotifyService runs (pg_notify, ~1ms latency, but if this
///                   service is down between disconnect and reconnect, signals are missed
///                   — though JetStream still buffers/replays via subject history)
///   Both         -> both services run. They share the same outbox_message.id as the NATS
///                   MsgId, so JetStream's 5-minute dedup window collapses duplicate
///                   publishes into one delivery. This is the recommended production
///                   setting: low latency AND zero message loss.
/// </summary>
public enum PublishMode
{
    Outbox,
    ListenNotify,
    Both
}



/// <summary>
/// Validates NatsOptions at startup (via AddOptions<>().ValidateOnStart()) rather
/// than letting a missing/bad config blow up on the first NATS connection attempt
/// deep inside request handling. Catches the common misconfigurations seen while
/// wiring up this cluster: empty Urls, missing APP credentials, accidentally
/// pointing at the SYS account.
/// </summary>
public class NatsOptionsValidator : IValidateOptions<NatsOptions>
{
    public ValidateOptionsResult Validate(string? name, NatsOptions options)
    {
        var errors = new List<string>();
 
        if (options.Urls is null || options.Urls.Length == 0)
        {
            errors.Add(
                "NATS:Urls must contain at least one seed URL. For the catalog-cluster, " +
                "configure all 3 nodes, e.g. [\"nats://nats-1:4222\", \"nats://nats-2:4222\", " +
                "\"nats://nats-3:4222\"].");
        }
        else
        {
            foreach (var url in options.Urls)
            {
                if (string.IsNullOrWhiteSpace(url) || !url.StartsWith("nats://"))
                {
                    errors.Add($"NATS:Urls contains an invalid entry: '{url}'. Expected format nats://host:port.");
                }
            }
        }
 
        if (string.IsNullOrWhiteSpace(options.Username))
        {
            errors.Add(
                "NATS:Username is required. Use the APP account (NATS_APP_USER), never SYS — " +
                "SYS has no JetStream access and will fail with error 10039.");
        }
        else if (options.Username.Equals("admin", StringComparison.OrdinalIgnoreCase)
                 || options.Username.Equals("sys", StringComparison.OrdinalIgnoreCase))
        {
            errors.Add(
                $"NATS:Username is '{options.Username}', which looks like the SYS account. " +
                "The API must connect as the APP account (JetStream-enabled), not SYS.");
        }
 
        if (string.IsNullOrWhiteSpace(options.Password))
        {
            errors.Add("NATS:Password is required.");
        }
 
        if (string.IsNullOrWhiteSpace(options.StreamName))
        {
            errors.Add("NATS:StreamName is required and must match the stream provisioned by " +
                        "infra/nats/create-stream.sh.");
        }
 
        if (options.DedupWindowMinutes <= 0)
        {
            errors.Add("NATS:DedupWindowMinutes must be greater than 0.");
        }
 
        return errors.Count > 0
            ? ValidateOptionsResult.Fail(errors)
            : ValidateOptionsResult.Success;
    }
}
 