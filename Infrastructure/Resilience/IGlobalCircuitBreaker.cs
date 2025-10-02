using Microsoft.Extensions.Logging;

namespace WorkOrderFunctions.Infrastructure.Resilience;

public interface IGlobalCircuitBreaker
{
    Task WaitIfActiveAsync(ILogger logger);
    Task OpenIfRetryAfterAsync(TimeSpan? retryAfter, ILogger logger);
}
