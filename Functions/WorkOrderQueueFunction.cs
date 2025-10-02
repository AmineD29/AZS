using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Azure.Messaging.ServiceBus;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;
using WorkOrderFunctions.Application.Orchestrators;

namespace WorkOrderFunctions.Functions
{
    public class WorkOrderQueueFunction
    {
        private readonly IWorkOrderOrchestrator _orchestrator;
        private readonly ILogger<WorkOrderQueueFunction> _logger;

        public WorkOrderQueueFunction(IWorkOrderOrchestrator orchestrator, ILogger<WorkOrderQueueFunction> logger)
        {
            _orchestrator = orchestrator;
            _logger = logger;
        }

        [Function("WorkOrderQueueToDynamics")]
        public async Task Run(
            // On passe du string à ServiceBusReceivedMessage pour accéder aux métadonnées (DeliveryCount, MessageId, etc.)
            [ServiceBusTrigger("workorder", Connection = "ServiceBusConnection")]
            ServiceBusReceivedMessage message,
            // Settlement explicite : Complete / DeadLetter / Abandon
            ServiceBusMessageActions actions,
            FunctionContext ctx)
        {
            // ✅ On conserve ta logique métier existante : l’orchestrateur reçoit le corps en string
            var msg = message.Body?.ToString() ?? string.Empty;

            // Scope de corrélation pour tous les logs de cette exécution
            using var _ = _logger.BeginScope(new Dictionary<string, object>
            {
                ["MessageId"] = message.MessageId,
                ["CorrelationId"] = message.CorrelationId,
                ["DeliveryCount"] = message.DeliveryCount,
                ["InvocationId"] = ctx.InvocationId
            });

            _logger.LogInformation("WorkorderQueueToDynamics triggered. Message length={Len}", msg.Length);

            // Aligne cette valeur avec le "Max delivery count" de la queue/subscription (côté Azure Service Bus)
            var maxDeliveryCount = TryGetIntFromEnv("ServiceBus:MaxDeliveryCount", 10);

            try
            {
                // ✅ Logique métier intacte (ne rien modifier ici)
                // Pas de parsing ici : l’orchestrateur décidera createOnly/upsert selon EventType dans le payload.
                await _orchestrator.ProcessAsync(msg, _logger);

                // Settlement explicite "safe" (intercepte MessageLockLost)
                await SafeCompleteAsync(actions, message, _logger);
            }
            catch (Exception ex)
            {
                var reason = "ProcessingFailed";
                var description = BuildDeadLetterDescription(ex, message, ctx);

                // 👉 Dead-letter uniquement "après les tentatives de retry"
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

                        // Ne pas relancer après un dead-letter explicite (OK ou lock perdu)
                        return;
                    }

                    // Échec DLQ hors MessageLockLost : on relance pour éviter toute perte
                    _logger.LogError(ex, "DeadLetterMessageAsync a échoué (hors MessageLockLost). On relance pour retry.");
                    throw;
                }

                // Pas encore au dernier essai => on laisse Service Bus réessayer
                _logger.LogWarning(ex,
                    "Échec de traitement, retry prévu. MessageId={MessageId}, DeliveryCount={DeliveryCount}/{MaxDeliveryCount}, InvocationId={InvocationId}",
                    message.MessageId,
                    message.DeliveryCount,
                    maxDeliveryCount,
                    ctx.InvocationId);

                throw;
            }
        }

        // --- Helpers ---

        private static int TryGetIntFromEnv(string name, int @default)
            => int.TryParse(Environment.GetEnvironmentVariable(name), out var v) ? v : @default;

        /// <summary>
        /// Construit une description compacte pour la DLQ (éviter d'y mettre des données sensibles).
        /// </summary>
        private static string BuildDeadLetterDescription(Exception ex, ServiceBusReceivedMessage msg, FunctionContext ctx)
        {
            var baseDesc = $"{ex.GetType().Name}: {ex.Message} | MsgId={msg.MessageId} | InvId={ctx.InvocationId}";
            return baseDesc.Length > 1024 ? baseDesc[..1024] : baseDesc;
        }

        /// <summary>
        /// Tente un Complete ; en cas de MessageLockLost (lock expiré ou message déjà settlé), log + ignore.
        /// </summary>
        private static async Task SafeCompleteAsync(ServiceBusMessageActions actions, ServiceBusReceivedMessage message, ILogger logger)
        {
            try
            {
                await actions.CompleteMessageAsync(message);
                logger.LogInformation("Message complété avec succès.");
            }
            catch (ServiceBusException sbEx) when (sbEx.Reason == ServiceBusFailureReason.MessageLockLost)
            {
                // Le lock a expiré juste avant le Complete, ou le message a déjà été settlé ailleurs.
                logger.LogWarning(sbEx, "Lock perdu lors du Complete (probablement déjà settlé). On ignore.");
                // Le message pourra être redélivré : s'assurer d'une logique métier idempotente.
            }
        }

        /// <summary>
        /// Tente un DeadLetter ; en cas de MessageLockLost, considère comme traité et log un warning.
        /// </summary>
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
                // Si le lock est perdu ici, il est probable que le message ait été settlé ou rejoué.
                logger.LogWarning(sbEx, "Lock perdu lors du DeadLetter (message probablement déjà settlé).");
                return true; // On considère traité pour éviter d'échouer la fonction.
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "DeadLetterMessageAsync a échoué (hors MessageLockLost).");
                return false;
            }
        }
    }
}