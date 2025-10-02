using WorkOrderFunctions.Config;

namespace WorkOrderFunctions.Infrastructure.Resilience;

public class GlobalBatchSemaphoreGate : IGlobalConcurrencyGate
{
    private static SemaphoreSlim _sem = new(100);

    public GlobalBatchSemaphoreGate(AppSettings settings)
    {
        if (settings.MaxConcurrentBatches > 0)
            _sem = new SemaphoreSlim(settings.MaxConcurrentBatches);
    }

    public Task WaitAsync() => _sem.WaitAsync();
    public void Release() => _sem.Release();
}
