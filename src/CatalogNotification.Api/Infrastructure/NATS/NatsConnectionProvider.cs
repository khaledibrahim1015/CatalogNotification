using Microsoft.Extensions.Options;
using NATS.Client.Core;
using NATS.Client.JetStream;
using NATS.Net;

namespace CatalogNotification.Api.Infrastructure.NATS;

public interface INatsConnectionProvider
{
    ValueTask<INatsConnection> GetConnectionAsync(CancellationToken ct = default);
    ValueTask<INatsJSContext> GetJetStreamContextAsync(CancellationToken ct = default);
}
 
/// <summary>
/// Owns the single NATS connection used by the API. Auth is the JetStream-enabled
/// "APP" account on the catalog-cluster — NOT the SYS account, which has no
/// JetStream access and will fail with "JetStream not enabled for account" (10039)
/// if used here by mistake.
/// </summary>
public sealed class NatsConnectionProvider : INatsConnectionProvider, IAsyncDisposable
{
    private readonly NatsOpts _opts;
    private readonly ILogger<NatsConnectionProvider> _logger;
    private NatsConnection? _connection;
    private INatsJSContext? _jsContext;
    private readonly SemaphoreSlim _semaphore = new(1, 1);
 
    public NatsConnectionProvider(IOptions<NatsOptions> options, ILogger<NatsConnectionProvider> logger)
    {
        _logger = logger;
        var opt = options.Value;
 
        if (opt.Urls is null || opt.Urls.Length == 0)
        {
            throw new InvalidOperationException(
                "NATS:Urls must contain at least one seed URL. " +
                "For the catalog-cluster, configure all 3 nodes for client-side failover, " +
                "e.g. [\"nats://nats-1:4222\", \"nats://nats-2:4222\", \"nats://nats-3:4222\"].");
        }
 
        if (string.IsNullOrWhiteSpace(opt.Username) || string.IsNullOrWhiteSpace(opt.Password))
        {
            throw new InvalidOperationException(
                "NATS:Username / NATS:Password are required. Use the APP account credentials " +
                "(NATS_APP_USER / NATS_APP_PASSWORD), not SYS — SYS has no JetStream access.");
        }
 
        // NatsOpts.Url accepts a single comma-separated string of seed servers — this is
        // where the configured array gets joined into the format the client expects.
        var joinedUrl = string.Join(",", opt.Urls);
 
        _opts = new NatsOpts
        {
            Url = joinedUrl,
            Name = "catalog-api",
            MaxReconnectRetry = -1, // infinite reconnects
            ConnectTimeout = TimeSpan.FromSeconds(5),
            PingInterval = TimeSpan.FromSeconds(20),
            MaxPingOut = 3,
            AuthOpts = new NatsAuthOpts
            {
                Username = opt.Username,
                Password = opt.Password
            }
        };
 
        _logger.LogInformation(
            "NATS configured with {Count} seed URL(s): {Urls}",
            opt.Urls.Length, joinedUrl);
    }
 
    public async ValueTask<INatsConnection> GetConnectionAsync(CancellationToken ct = default)
    {
        if (_connection is { ConnectionState: NatsConnectionState.Open })
        {
            return _connection;
        }
 
        await _semaphore.WaitAsync(ct);
        try
        {
            if (_connection is null || _connection.ConnectionState != NatsConnectionState.Open)
            {
                _connection = new NatsConnection(_opts);
                await _connection.ConnectAsync();
                _logger.LogInformation("Connected to NATS as APP account at {Url}", _opts.Url);
            }
            return _connection;
        }
        finally
        {
            _semaphore.Release();
        }
    }
 
    public async ValueTask<INatsJSContext> GetJetStreamContextAsync(CancellationToken ct = default)
    {
        if (_jsContext is not null)
        {
            return _jsContext;
        }
 
        var conn = await GetConnectionAsync(ct);
        _jsContext = conn.CreateJetStreamContext();
        return _jsContext;
    }
 
    public async ValueTask DisposeAsync()
    {
        if (_connection is not null)
        {
            await _connection.DisposeAsync();
        }
        _semaphore.Dispose();
    }
}
 
