using Azure.Messaging.EventGrid;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;
using WorkOrderFunctions.Infrastructure.Messaging;

namespace WorkOrderFunctions.Functions
{
    public class WorkOrderDispatcherFunction
    {
        private readonly DispatcherService _dispatcher;
        private readonly ILogger<WorkOrderDispatcherFunction> _logger;

        public WorkOrderDispatcherFunction(DispatcherService dispatcher, ILogger<WorkOrderDispatcherFunction> logger)
        {
            _dispatcher = dispatcher;
            _logger = logger;
        }

        [Function("WorkOrderDispatcher")]
        public async Task RunDispatcher([EventGridTrigger] EventGridEvent ev, FunctionContext context)
        {
            try
            {
                _logger.LogInformation("Dispatcher start | EventId={Id} Type={Type} Subject={Subject} DataSize={Size}",
                    ev.Id, ev.EventType, ev.Subject, ev.Data?.ToString()?.Length ?? 0);

                await _dispatcher.DispatchAsync(ev, _logger);

                _logger.LogInformation("Dispatcher done | EventId={Id}", ev.Id);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex,
                    "Dispatcher failed | EventId={Id} Type={Type} Subject={Subject}. Error={Message}",
                    ev.Id, ev.EventType, ev.Subject, ex.Message);
                throw; // laisse la plateforme gérer le retry
            }
        }
    }
}
