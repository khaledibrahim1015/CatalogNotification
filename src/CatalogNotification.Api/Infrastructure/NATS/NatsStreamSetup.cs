using Microsoft.Extensions.Options;
using NATS.Client.JetStream;
using NATS.Client.JetStream.Models;

namespace CatalogNotification.Api.Infrastructure.NATS;


/// <summary>
/// Selects retention/replay semantics for reconnecting mobile clients.
/// Mirrors infra/nats/create-stream.sh — this is the single source of truth
/// for desired stream shape; EnsureStreamAndConsumersAsync below creates or
/// updates the stream to match it on startup, the same way the script does.
///
///   LastOnly     -> MaxMsgsPerSubject=1   (only latest catalog state kept)
///   AllMissed    -> MaxMsgsPerSubject=-1  (full history, replay every change)
///   TimeBounded  -> AllMissed + MaxAge=N  (configurable retention window)
/// </summary>
public enum MissedMessagePolicy
{
    LastOnly,
    AllMissed,
    TimeBounded
}

public interface INatsStreamSetup
{
    Task EnsureStreamAsync(CancellationToken ct = default);
}
 
/// <summary>
/// Verifies the stream this app depends on actually exists with the expected
/// shape. Does NOT create it from scratch in normal operation — stream
/// provisioning is owned by infra/nats/create-stream.sh so that replicas,
/// storage, and policy stay under explicit ops control (see PublishMode
/// remarks below for why duplicate-window correctness matters here).
///
/// If the stream is missing entirely (e.g. fresh environment with infra
/// scripts not yet run), this throws rather than silently creating a stream
/// with app-guessed settings that could drift from the cluster's actual
/// account/replica/storage configuration.
/// </summary>
/// <summary>
/// Creates the stream this app depends on if it doesn't exist, or updates it
/// in place to match the expected shape if it does — same idempotent
/// create-or-update behavior as infra/nats/create-stream.sh, just expressed
/// via the .NET JetStream client instead of the `nats` CLI.
///
/// Config here (subjects, storage=file, replicas=3, dupe-window, discard
/// policy, etc.) must stay in sync with create-stream.sh manually if you
/// still use that script for manual/ops-driven provisioning elsewhere.
/// </summary>
public class NatsStreamSetup : INatsStreamSetup
{
    private readonly INatsConnectionProvider _provider;
    private readonly ILogger<NatsStreamSetup> _logger;
    private readonly NatsOptions _options;
 
    public NatsStreamSetup(
        INatsConnectionProvider provider,
        ILogger<NatsStreamSetup> logger,
        IOptions<NatsOptions> options)
    {
        _provider = provider;
        _logger = logger;
        _options = options.Value;
    }
 
    public async Task EnsureStreamAsync(CancellationToken ct = default)
    {
   
        var js = await _provider.GetJetStreamContextAsync(ct);
        var streamName = _options.StreamName;
        var desired = BuildDesiredConfig(
            streamName,
            _options.SubjectPrefix,
            _options.DedupWindowMinutes,
            _options.MissedMessagePolicy,
            _options.TimeBoundedMaxAge);
        
        try
        {
            var stream = await js.GetStreamAsync(streamName, cancellationToken: ct);
 
            _logger.LogInformation(
                "Stream {StreamName} exists ({MsgCount} messages) — updating to match policy={Policy}",
                streamName, stream.Info.State?.Messages, _options.MissedMessagePolicy);
 
            await stream.UpdateAsync(desired, cancellationToken: ct);
 
            _logger.LogInformation("Stream {StreamName} updated.", streamName);
        }
        catch (NatsJSApiException ex) when (ex.Error.Code == 404)
        {
            _logger.LogInformation(
                "Stream {StreamName} does not exist — creating with policy={Policy}",
                streamName, _options.MissedMessagePolicy);
 
            await js.CreateStreamAsync(desired, cancellationToken: ct);
 
            _logger.LogInformation("Stream {StreamName} created.", streamName);
        }
    }
 
    /// <summary>
    /// Defensive check: warns loudly if the deployed stream doesn't match the
    /// production-safe shape (3 replicas, file storage) the cluster was
    /// designed for. Catches config drift between infra scripts and what's
    /// actually running, e.g. someone manually editing the stream with
    /// `nats stream edit` and forgetting --replicas/--storage constraints.
    /// </summary>
    private void ValidateProductionShape(StreamConfig? config)
    {
        if (config is null) return;
 
        if (config.NumReplicas != 3)
        {
            _logger.LogWarning(
                "Stream {StreamName} has {Replicas} replicas, expected 3 for this 3-node cluster. " +
                "Even replica counts (e.g. 2) do not improve Raft quorum tolerance — " +
                "use 1 or 3 in a 3-node deployment.",
                config.Name, config.NumReplicas);
        }
 
        if (config.Storage != StreamConfigStorage.File)
        {
            _logger.LogWarning(
                "Stream {StreamName} uses {Storage} storage. File storage is expected for " +
                "durability across node restarts — Memory storage loses all messages on restart.",
                config.Name, config.Storage);
        }
    }
    
    /// <summary>
    /// Mirrors the flags passed to `nats stream add` / `nats stream edit`
    /// in create-stream.sh. Replicas=3 + Storage=File are fixed (production
    /// shape for the 3-node cluster); only the retention knobs vary by
    /// MissedMessagePolicy.
    /// </summary>
    private static StreamConfig BuildDesiredConfig(
        string streamName,
        string subjectPrefix,
        int dedupWindowMinutes,
        MissedMessagePolicy policy,
        TimeSpan? timeBoundedMaxAge)
    {
        var (maxMsgsPerSubject, maxAge) = policy switch
        {
            MissedMessagePolicy.LastOnly => (1L, TimeSpan.Zero),
            MissedMessagePolicy.AllMissed => (-1L, TimeSpan.Zero),
            MissedMessagePolicy.TimeBounded => (-1L, timeBoundedMaxAge
                                                     ?? throw new InvalidOperationException(
                                                         $"{nameof(NatsOptions.TimeBoundedMaxAge)} must be set when " +
                                                         $"{nameof(NatsOptions.MissedMessagePolicy)} is {nameof(MissedMessagePolicy.TimeBounded)}.")),
            _ => throw new ArgumentOutOfRangeException(nameof(policy), policy, null)
        };
 
        return new StreamConfig
        {
            Name = streamName,
            Subjects = new[] { $"{subjectPrefix}.>" },
            Storage = StreamConfigStorage.File,
            NumReplicas = 3,
            Retention = StreamConfigRetention.Limits,
            Discard = StreamConfigDiscard.Old ,
            MaxMsgsPerSubject = maxMsgsPerSubject,
            MaxMsgs = -1,
            MaxBytes = -1,
            MaxMsgSize = -1,
            MaxAge = maxAge,
            DuplicateWindow = TimeSpan.FromMinutes(dedupWindowMinutes),
            Compression = StreamConfigCompression.S2,
            AllowRollupHdrs = false,  // --no-allow-rollup
            DenyDelete = true,        // --deny-delete
            AllowDirect = true        // --allow-direct
        };
    }
}
 
