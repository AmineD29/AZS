namespace WorkOrderFunctions.Infrastructure.Caching;

public interface IRedisCache
{
    Task<string?> GetStringAsync(string key);
    Task SetStringAsync(string key, string value, TimeSpan? ttl = null);

    Task<(bool Acquired, string Token)> TryAcquireLockAsync(string key, TimeSpan ttl);
    Task ReleaseLockAsync(string key, string token);
    //Task<bool> TryMarkAsProcessedAsync(string key, TimeSpan ttl);
}
