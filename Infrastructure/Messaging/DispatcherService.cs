using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using Azure.Messaging.EventGrid;
using Azure.Messaging.ServiceBus;
using Microsoft.Extensions.Logging;
using WorkOrderFunctions.Domain.DTOs;
using WorkOrderFunctions.Domain.Entities;

namespace WorkOrderFunctions.Infrastructure.Messaging
{
    public class DispatcherService
    {
        private readonly IServiceBusSenderFactory _factory;

        public DispatcherService(IServiceBusSenderFactory factory) => _factory = factory;

        public async Task DispatchAsync(EventGridEvent eventGridEvent, ILogger logger)
        {
            WorkOrderPayload payload;
            try
            {
                payload = JsonSerializer.Deserialize<WorkOrderPayload>(
                    eventGridEvent.Data.ToString()!,
                    new JsonSerializerOptions { PropertyNameCaseInsensitive = true })!;
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "Failed to deserialize EventGrid data | EventId={Id} Type={Type} RawData={Raw}",
                    eventGridEvent.Id, eventGridEvent.EventType, Truncate(eventGridEvent.Data?.ToString(), 2048));
                return; // on abandonne cet event
            }

            if (payload?.WorkOrders == null || payload.WorkOrders.Count == 0)
            {
                logger.LogWarning("Empty or invalid payload (no WorkOrders) | EventId={Id}", eventGridEvent.Id);
                return;
            }

            int parentsPerBatch = int.TryParse(Environment.GetEnvironmentVariable("ParentsPerBatch"), out var n)
                ? Math.Max(1, n) : 1;

            int maxRequestsPerBatch = int.TryParse(Environment.GetEnvironmentVariable("MaxRequestsPerBatch"), out var r)
                ? Math.Min(Math.Max(10, r), 1000) : 1000;

            //var evType = (eventGridEvent.EventType ?? string.Empty).ToLowerInvariant();
            string queueName = "workorder";

            logger.LogInformation("Dispatch routing | EventId={Id} Type={Type} -> Queue={Queue} Parents={Count}",
                eventGridEvent.Id, eventGridEvent.EventType, queueName, payload.WorkOrders.Count);

            await using var sender = _factory.CreateSender(queueName);

            int i = 0;
            while (i < payload.WorkOrders.Count)
            {
                int requests = 0;
                var group = new List<WorkOrder>();

                while (i < payload.WorkOrders.Count && group.Count < parentsPerBatch)
                {
                    var parent = payload.WorkOrders[i];
                    int reqForParent = 1 + (parent.SubWorkOrders?.Count ?? 0);

                    if (requests + reqForParent > maxRequestsPerBatch && group.Count > 0)
                        break;

                    requests += reqForParent;
                    group.Add(parent);
                    i++;
                }

                var messagePayload = new WorkOrderPayload
                {
                    EventType = eventGridEvent.EventType,
                    WorkOrders = group
                };

                string messageId = BuildDeterministicMessageId(eventGridEvent.Id, eventGridEvent.EventType, group);
                var body = JsonSerializer.Serialize(messagePayload);

                var message = new ServiceBusMessage(body)
                {
                    MessageId = messageId,
                    CorrelationId = eventGridEvent.Id,
                    ContentType = "application/json"
                };

                try
                {
                    await sender.SendMessageAsync(message);
                    logger.LogInformation("SB send OK | Queue={Queue} Parents={Parents} Requests={Req} MessageId={MsgId}",
                        queueName, group.Count, requests, messageId);
                }
                catch (Exception ex)
                {
                    logger.LogError(ex, "SB send FAILED | Queue={Queue} MessageId={MsgId}. FirstParent={FirstParent}",
                        queueName, messageId, group.FirstOrDefault()?.WorkOrderBroadcastId);
                    throw;
                }
            }
        }

        private static string BuildDeterministicMessageId(string eventId, string? eventType, List<WorkOrder> parents)
        {
            var parentsSorted = parents
                .Select(p => p.WorkOrderBroadcastId)
                .Where(x => !string.IsNullOrWhiteSpace(x))
                .OrderBy(x => x, StringComparer.Ordinal)
                .ToArray();

            var raw = $"{eventId}|{eventType}|{string.Join(",", parentsSorted)}";
            using var sha = SHA256.Create();
            var hash = sha.ComputeHash(Encoding.UTF8.GetBytes(raw));
            return Convert.ToHexString(hash);
        }

        private static string Truncate(string? s, int max)
            => string.IsNullOrEmpty(s) ? string.Empty : (s.Length <= max ? s : s.Substring(0, max) + "...");
    }
}
