namespace WorkOrderFunctions.Infrastructure.Resilience;

public interface IGlobalConcurrencyGate
{
    Task WaitAsync();
    void Release();
}
