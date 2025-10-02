using Microsoft.Extensions.Logging;

namespace WorkOrderCreate.Application.Orchestrators
{
    public interface IRessourceOrchestrator
    {
        Task ProcessAsync(string queueMessage, ILogger logger);
        Task ProcessAsync(string queueMessage, ILogger logger, bool createOnly);
        }
}
