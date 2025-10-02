using Microsoft.Extensions.Logging;
using WorkOrderFunctions.Infrastructure.Dataverse;

namespace WorkOrderFunctions.Policies;

public interface IBatchRetryPolicy
{
    Task<BatchExecutionResult> ExecuteAsync(
        Func<List<(Dictionary<string, object> Payload, string EntitySetName, int ParentContentId)>, Task<BatchExecutionResult>> executeBatch,
        List<(Dictionary<string, object> Payload, string EntitySetName, int ParentContentId)> operations,
        ILogger logger,
        CancellationToken ct = default);
}
