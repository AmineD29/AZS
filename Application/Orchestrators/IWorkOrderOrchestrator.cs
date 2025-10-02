using Microsoft.Extensions.Logging;

namespace WorkOrderFunctions.Application.Orchestrators
{
    public interface IWorkOrderOrchestrator
    {
        Task ProcessAsync(string queueMessage, ILogger logger, bool createOnly);
        Task ProcessAsync(string queueMessage, ILogger logger);
    }
}
