using StackExchange.Redis;

namespace WorkOrderFunctions.Infrastructure.Caching;

public class RedisCache : IRedisCache
{
    private readonly IDatabase _redis;
    private static readonly Lazy<ConnectionMultiplexer> _lazyConnection = new(() =>
    {
        string host = Environment.GetEnvironmentVariable("RedisHost") ?? throw new ArgumentNullException("RedisHost");
        string password = Environment.GetEnvironmentVariable("RedisPassword") ?? throw new ArgumentNullException("RedisPassword");
        var options = new ConfigurationOptions
        {
            EndPoints = { host },
            Password = password,
            Ssl = true,
            ConnectRetry = 5,
            AbortOnConnectFail = false,
            ConnectTimeout = 15000,
            SyncTimeout = 15000,
            KeepAlive = 60
        };
        return ConnectionMultiplexer.Connect(options);
    });

    private readonly LuaScript _releaseScript = LuaScript.Prepare(
        @"if redis.call('GET', KEYS[1]) == ARGV[1] then
              return redis.call('DEL', KEYS[1])
          else
              return 0
          end");

    public RedisCache()
    {
        _redis = _lazyConnection.Value.GetDatabase();
    }

    public async Task<string?> GetStringAsync(string key)
    {
        var v = await _redis.StringGetAsync(key);
        return v.IsNullOrEmpty ? null : v.ToString();
    }

    public Task SetStringAsync(string key, string value, TimeSpan? ttl = null)
        => _redis.StringSetAsync(key, value, ttl);

    public async Task<(bool Acquired, string Token)> TryAcquireLockAsync(string key, TimeSpan ttl)
    {
        var token = Guid.NewGuid().ToString();
        var ok = await _redis.StringSetAsync(key, token, ttl, When.NotExists);
        return (ok, token);
    }

    public Task ReleaseLockAsync(string key, string token)
        => _redis.ScriptEvaluateAsync(_releaseScript, new { KEYS = new[] { key }, ARGV = new[] { token } });

    //public async Task<bool> TryMarkAsProcessedAsync(string key, TimeSpan ttl)
    //{
    //    // Script Lua : SETNX + EXPIRE atomique
    //    string lua = @"
    //        if redis.call('SETNX', KEYS[1], '1') == 1 then
    //            redis.call('EXPIRE', KEYS[1], ARGV[1])
    //            return 1
    //        else
    //            return 0
    //        end";
    //    var result = (int)await _redis.ScriptEvaluateAsync(
    //        lua,
    //        new RedisKey[] { key },
    //        new RedisValue[] { (int)ttl.TotalSeconds }
    //    );
    //    return result == 1;
    //}

}
