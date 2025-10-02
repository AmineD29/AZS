using Microsoft.Extensions.Logging;
using WorkOrderFunctions.Infrastructure.Caching;

namespace WorkOrderFunctions.Infrastructure.Resilience;

public class RedisCircuitBreaker : IGlobalCircuitBreaker
{
    private readonly IRedisCache _redis;
    public RedisCircuitBreaker(IRedisCache redis) => _redis = redis;

    public async Task WaitIfActiveAsync(ILogger logger)
    {
        string key = "dataverse:circuitbreaker";
        var breakerUntilStr = await _redis.GetStringAsync(key);
        if (!string.IsNullOrEmpty(breakerUntilStr))
        {
            if (DateTime.TryParse(breakerUntilStr, out var breakerUntil) && DateTime.UtcNow < breakerUntil)
            {
                var wait = breakerUntil - DateTime.UtcNow;
                logger.LogWarning("[CircuitBreaker] Pause globale jusqu'à {Until:u} ({Seconds}s)", breakerUntil, Math.Max(0, (int)wait.TotalSeconds));
                await Task.Delay(wait);
            }
        }
    }

    public async Task OpenIfRetryAfterAsync(TimeSpan? retryAfter, ILogger logger)
    {
        var minPause = TimeSpan.FromSeconds(5);
        var maxPause = TimeSpan.FromSeconds(60);
        logger.LogWarning($"Retry after value {retryAfter}");
        var pause = retryAfter.HasValue && retryAfter.Value > TimeSpan.Zero
            ? retryAfter.Value
            : minPause;

        if (pause > maxPause && !retryAfter.HasValue)
            pause = maxPause;

        var until = DateTime.UtcNow.Add(pause);
        await _redis.SetStringAsync("dataverse:circuitbreaker", until.ToString("o"), pause);
        logger.LogWarning("[CircuitBreaker] Pause globale jusqu'à {Until:u} ({Seconds}s)", until, (int)pause.TotalSeconds);
    }
}
