// src/WorkOrderFunctions.Functions/MobilierQueueFunction.cs
using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Azure.Messaging.ServiceBus;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;
using WorkOrderFunctions.Application.Orchestrators;

namespace WorkOrderFunctions.Functions
{
    public sealed class MobilierQueueFunction
    {
        private readonly IMobilierOrchestrator _orchestrator;
        private readonly ILogger<MobilierQueueFunction> _logger;

        public MobilierQueueFunction(IMobilierOrchestrator orchestrator, ILogger<MobilierQueueFunction> logger)
        {
            _orchestrator = orchestrator;
            _logger = logger;
        }

        [Function("MobilierQueueToDynamics")]
        public async Task Run(
            [ServiceBusTrigger("mobilier", Connection = "ServiceBusConnection")]
            ServiceBusReceivedMessage message,
            ServiceBusMessageActions actions,
            FunctionContext ctx)
        {
            var msg = message.Body?.ToString() ?? string.Empty;

            using var _ = _logger.BeginScope(new Dictionary<string, object>
            {
                ["MessageId"] = message.MessageId,
                ["CorrelationId"] = message.CorrelationId,
                ["DeliveryCount"] = message.DeliveryCount,
                ["InvocationId"] = ctx.InvocationId
            });

            _logger.LogInformation("MobilierQueueToDynamics triggered. Message length={Len}", msg.Length);

            var maxDeliveryCount = int.TryParse(Environment.GetEnvironmentVariable("ServiceBus:MaxDeliveryCount"), out var cfg)
                ? cfg
                : 10;

            try
            {
                // Logique métier intacte
                await _orchestrator.ProcessAsync(msg, _logger);

                // Settlement explicite et "safe"
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
                        return; // ne pas relancer après un DLQ explicite (OK ou lock perdu)
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