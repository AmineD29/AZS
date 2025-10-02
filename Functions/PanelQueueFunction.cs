using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Azure.Messaging.ServiceBus;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;
using WorkOrderFunctions.Application.Orchestrators;

namespace WorkOrderFunctions.Functions
{
    public class PanelQueueFunction
    {
        private readonly IPanelOrchestrator _orchestrator;
        private readonly ILogger<PanelQueueFunction> _logger;

        public PanelQueueFunction(IPanelOrchestrator orchestrator, ILogger<PanelQueueFunction> logger)
        {
            _orchestrator = orchestrator;
            _logger = logger;
        }

        // Déclenchée à la réception d’un message sur la file SB "panel"
        [Function("PanelQueueToDynamics")]
        public async Task Run(
            [ServiceBusTrigger("panel", Connection = "ServiceBusConnection")]
            ServiceBusReceivedMessage message,
            ServiceBusMessageActions actions,
            FunctionContext ctx)
        {
            //  On conserve la logique métier : l’orchestrateur reçoit le corps en string
            var msg = message.Body?.ToString() ?? string.Empty;

            using var _ = _logger.BeginScope(new Dictionary<string, object>
            {
                ["MessageId"] = message.MessageId,
                ["CorrelationId"] = message.CorrelationId,
                ["DeliveryCount"] = message.DeliveryCount,
                ["InvocationId"] = ctx.InvocationId
            });

            _logger.LogInformation("PanelQueueToDynamics triggered. Message length={Len}", msg.Length);

            // Aligne avec "Max delivery count" de la queue/subscription
            var maxDeliveryCount = TryGetIntFromEnv("ServiceBus:MaxDeliveryCount", 10);

            try
            {
                await _orchestrator.ProcessAsync(msg, _logger);
                await SafeCompleteAsync(actions, message, _logger);
            }
            catch (Exception ex)
            {
                var reason = "ProcessingFailed";
                var description = BuildDeadLetterDescription(ex, message, ctx);

                if (message.DeliveryCount >= maxDeliveryCount)
                {
                    var deadLettered = await SafeDeadLetterAsync(actions, message, reason, description, _logger);

                    if (deadLettered)
                    {
                        _logger.LogError(ex,
                            "DLQ => MessageId={MessageId}, CorrelationId={CorrelationId}, Subject={Subject}, DeliveryCount={DeliveryCount}/{MaxDeliveryCount}, EnqueuedTime={EnqueuedTime:o}, Reason={Reason}, Description={Description}, InvocationId={InvocationId}",
                            message.MessageId,
                            message.CorrelationId,
                            message.Subject,
                            message.DeliveryCount,
                            maxDeliveryCount,
                            message.EnqueuedTime,
                            reason,
                            description,
                            ctx.InvocationId);
                        return; // ne pas relancer après un DLQ explicite
                    }

                    _logger.LogError(ex, "DeadLetterMessageAsync a échoué (hors MessageLockLost). On relance pour retry.");
                    throw;
                }

                _logger.LogWarning(ex,
                    "Échec de traitement, retry prévu. MessageId={MessageId}, DeliveryCount={DeliveryCount}/{MaxDeliveryCount}, InvocationId={InvocationId}",
                    message.MessageId, message.DeliveryCount, maxDeliveryCount, ctx.InvocationId);

                throw; // abandon => redelivery
            }
        }

        private static int TryGetIntFromEnv(string name, int @default)
            => int.TryParse(Environment.GetEnvironmentVariable(name), out var v) ? v : @default;

        private static string BuildDeadLetterDescription(Exception ex, ServiceBusReceivedMessage msg, FunctionContext ctx)
        {
            var baseDesc = $"{ex.GetType().Name}: {ex.Message} | MsgId={msg.MessageId} | InvId={ctx.InvocationId}";
            return baseDesc.Length > 1024 ? baseDesc[..1024] : baseDesc;
        }

        private static async Task SafeCompleteAsync(ServiceBusMessageActions actions, ServiceBusReceivedMessage message, ILogger logger)
        {
            try
            {
                await actions.CompleteMessageAsync(message);
                logger.LogInformation("Message complété avec succès.");
            }
            catch (ServiceBusException sbEx) when (sbEx.Reason == ServiceBusFailureReason.MessageLockLost)
            {
                logger.LogWarning(sbEx, "Lock perdu lors du Complete (probablement déjà settlé). On ignore.");
            }
        }

        private static async Task<bool> SafeDeadLetterAsync(
            ServiceBusMessageActions actions,
            ServiceBusReceivedMessage message,
            string reason,
            string description,
            ILogger logger)
        {
            try
            {
                await actions.DeadLetterMessageAsync(
                    message,
                    deadLetterReason: reason,
                    deadLetterErrorDescription: description);

                logger.LogInformation("Message envoyé en DLQ (Reason={Reason}).", reason);
                return true;
            }
            catch (ServiceBusException sbEx) when (sbEx.Reason == ServiceBusFailureReason.MessageLockLost)
            {
                logger.LogWarning(sbEx, "Lock perdu lors du DeadLetter (message probablement déjà settlé).");
                return true;
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "DeadLetterMessageAsync a échoué (hors MessageLockLost).");
                return false;
            }
        }
    }
}
