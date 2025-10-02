using Microsoft.Extensions.Logging;

namespace WorkOrderFunctions.Application.Orchestrators;
public interface IPanelOrchestrator
{
    // Surcharges pour s'aligner à PanelQueueFunction (Partie 4/8)
    Task ProcessAsync(string queueMessage, ILogger logger);
    Task ProcessAsync(string queueMessage, ILogger logger, bool createOnly);
}