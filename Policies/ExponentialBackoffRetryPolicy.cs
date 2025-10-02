using Microsoft.Extensions.Logging;
using WorkOrderFunctions.Config;
using WorkOrderFunctions.Infrastructure.Dataverse;
using WorkOrderFunctions.Infrastructure.Resilience;

namespace WorkOrderFunctions.Policies;

public class ExponentialBackoffRetryPolicy : IBatchRetryPolicy
{
    private readonly IGlobalCircuitBreaker _breaker;
    private readonly AppSettings _settings;

    public ExponentialBackoffRetryPolicy(IGlobalCircuitBreaker breaker, AppSettings settings)
    {
        _breaker = breaker;
        _settings = settings;
    }

    public async Task<BatchExecutionResult> ExecuteAsync(
        Func<List<(Dictionary<string, object> Payload, string EntitySetName, int ParentContentId)>, Task<BatchExecutionResult>> executeBatch,
        List<(Dictionary<string, object> Payload, string EntitySetName, int ParentContentId)> operations,
        ILogger logger,
        CancellationToken ct = default)
    {
        var maxAttempts = _settings.BatchMaxAttempts > 0 ? _settings.BatchMaxAttempts : 5;
        var backoff     = TimeSpan.FromSeconds(_settings.BatchInitialBackoffSeconds > 0 ? _settings.BatchInitialBackoffSeconds : 1);
        var maxBackoff  = TimeSpan.FromSeconds(_settings.BatchMaxBackoffSeconds > 0 ? _settings.BatchMaxBackoffSeconds : 30);

        var rng = new Random();
        var attempt = 0;
        var pending = new List<(Dictionary<string, object>, string, int)>(operations);
        BatchExecutionResult? last = null;

        while (attempt < maxAttempts && pending.Count > 0)
        {
            ct.ThrowIfCancellationRequested();
            await _breaker.WaitIfActiveAsync(logger);
            attempt++;

            try
            {
                var resp = await executeBatch(pending);
                last = resp;

                if (resp.IsSuccess && (resp.FailedIndices == null || resp.FailedIndices.Count == 0))
                {
                    return resp; // succès complet
                }

                if (resp.IsThrottled)
                {
                    await _breaker.OpenIfRetryAfterAsync(resp.RetryAfter, logger);
                    var jitter = TimeSpan.FromMilliseconds(rng.Next(0, 500));
                    await Task.Delay(backoff + jitter, ct);
                    backoff = TimeSpan.FromSeconds(Math.Min(backoff.TotalSeconds * 2, maxBackoff.TotalSeconds));
                    continue; // rejouer le même pending
                }

                if (resp.FailedIndices is { Count: > 0 })
                {
                    // Mode atomique: on rejoue le lot complet (parents + enfants)
                    await Task.Delay(TimeSpan.FromMilliseconds(500), ct);
                    continue;
                }

                break; // échec permanent
            }
            catch (Exception)
            {
                var jitter = TimeSpan.FromMilliseconds(rng.Next(0, 500));
                await Task.Delay(backoff + jitter, ct);
                backoff = TimeSpan.FromSeconds(Math.Min(backoff.TotalSeconds * 2, maxBackoff.TotalSeconds));
            }
        }

        last ??= new BatchExecutionResult(
            IsSuccess: false,
            IsThrottled: false,
            SuccessCount: 0,
            FailedIndices: new List<int>(),
            ErrorMessage: "Max attempts reached or unknown error.",
            RetryAfter: null);

        return last;
    }
}
