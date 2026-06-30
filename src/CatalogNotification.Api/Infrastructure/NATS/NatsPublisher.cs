using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using Microsoft.Extensions.Options;
using NATS.Client.JetStream;

namespace CatalogNotification.Api.Infrastructure.NATS;

// interface 




public interface INatsPublisher
{
    // /// <summary>
    // /// Publish  to the AllMissed stream
    // /// </summary
    // Task PublishAsync (string subject , object payload , string? dedupId = null , CancellationToken ct = default);

    /// <summary>
    /// Publish only to the latest-only stream Not  AllMissed Stream 
    /// </summary
    Task PublishLatestOnlyAsync (string subject , object payload , string dedupId  , CancellationToken ct = default);
}

public class NatsPublisher : INatsPublisher
{
    
    private readonly INatsConnectionProvider _natsConnectionProvider;
    private readonly NatsOptions _natsOptions;
    private readonly ILogger<NatsPublisher> _logger;
    
    // will replace later with messagepack for performance 
    private readonly JsonSerializerOptions _jsonOpts;
    
    
    public NatsPublisher(
        INatsConnectionProvider provider,
        IOptions<NatsOptions> options,
        ILogger<NatsPublisher> logger)
    {
        _natsConnectionProvider = provider;
        _natsOptions = options.Value;
        _logger = logger;
        _jsonOpts = new JsonSerializerOptions
        {
            PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
            DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull
        };
    }
    
    
    public async  Task PublishLatestOnlyAsync(string subject, object payload, string? dedupId = null, CancellationToken ct = default)
    {
        var json = JsonSerializer.Serialize(payload, _jsonOpts);
        var bytes = Encoding.UTF8.GetBytes(json);
        
        var js = await _natsConnectionProvider.GetJetStreamContextAsync
                  ( ct);

        
        const int maxAttempts = 3;
        var delay = TimeSpan.FromMilliseconds(200);
        Exception? lastException = null;
        
        for (var attempt = maxAttempts; attempt >= 1; attempt--)
        {
            try
            {
                var ack = await js.PublishAsync(
                    subject: subject,
                    data: bytes,
                    opts: new NatsJSPubOpts
                    {
                        MsgId = dedupId,
                    },
                    cancellationToken: ct
                );
                ack.EnsureSuccess();

                if(ack.Duplicate)
                    _logger.LogInformation(
                        $"Publish deduplicated by NATS (already delivered by the other path): subject={subject} msgId={dedupId}  seq={ack.Seq}, stream={ack.Stream}, dup={ack.Duplicate}");
                else
                    _logger.LogInformation(
                        $"Published to Catalog Stream : subject={subject}, seq={ack.Seq}, stream={ack.Stream}, dup={ack.Duplicate}");
            }
            catch (Exception ex)
            {
                lastException = ex;

                if (attempt < maxAttempts)
                {
                    _logger.LogWarning(ex,
                        "NATS publish attempt {Attempt}/{Max} failed for msgId={MsgId}, retrying in {Delay}ms",
                        attempt, maxAttempts, dedupId, delay.TotalMilliseconds);

                    await Task.Delay(delay, ct);
                    delay *= 2; // exponential backoff
                }
            }
        
        }

        _logger.LogError(lastException,
            "NATS publish failed after {Max} attempts for msgId={MsgId}, subject={Subject}",
            maxAttempts, dedupId, subject);

        throw lastException ?? new InvalidOperationException("NATS publish failed for unknown reason");
        
        
        
    }
    
    
    public Task PublishAsync(string subject, object payload, string? dedupId = null, CancellationToken ct = default)
    {
        throw new NotImplementedException();
    }


}
