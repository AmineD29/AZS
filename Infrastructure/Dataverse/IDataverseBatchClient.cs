using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace WorkOrderFunctions.Infrastructure.Dataverse
{
    public record BatchExecutionResult(
        bool IsSuccess,
        bool IsThrottled,
        int SuccessCount,
        List<int> FailedIndices,
        string? ErrorMessage,
        TimeSpan? RetryAfter);

    public interface IDataverseBatchClient
    {
        Task<BatchExecutionResult> ExecuteBatchAsync(
            List<(Dictionary<string, object> Payload, string EntitySetName, int ParentContentId)> operations,
            string token,
            ILogger logger,
            bool createOnly);
       
    }
}
