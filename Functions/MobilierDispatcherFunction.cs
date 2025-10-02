// src/WorkOrderFunctions.Functions/MobilierDispatcherFunction.cs
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using Azure.Messaging.EventGrid;
using Azure.Messaging.ServiceBus;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;
using WorkOrderFunctions.Domain.DTOs;
using WorkOrderFunctions.Domain.Entities;
using WorkOrderFunctions.Infrastructure.Messaging;

namespace WorkOrderFunctions.Functions
{
    public sealed class MobilierDispatcherFunction
    {
        private readonly IServiceBusSenderFactory _senderFactory;
        private readonly ILogger<MobilierDispatcherFunction> _logger;

        public MobilierDispatcherFunction(IServiceBusSenderFactory senderFactory,
                                          ILogger<MobilierDispatcherFunction> logger)
        {
            _senderFactory = senderFactory;
            _logger = logger;
        }

        [Function("MobilierDispatcher")]
        public async Task Run([EventGridTrigger] EventGridEvent ev, FunctionContext ctx)
        {
            _logger.LogInformation(
                "MobilierDispatcher start | EventId={Id} Type={Type} Subject={Subject}",
                ev.Id, ev.EventType, ev.Subject);

            // 1) Extraire data.mobiliers (tableau)
            List<Mobilier> mobiliers;
            try
            {
                using var doc = JsonDocument.Parse(ev.Data?.ToString() ?? "{}");
                if (!doc.RootElement.TryGetProperty("mobiliers", out var arr) ||
                    arr.ValueKind != JsonValueKind.Array)
                {
                    _logger.LogWarning("MobilierDispatcher: payload invalide (data.mobiliers manquant). EventId={Id}", ev.Id);
                    return;
                }

                mobiliers = new List<Mobilier>(arr.GetArrayLength());
                var opts = new JsonSerializerOptions { PropertyNameCaseInsensitive = true };
                foreach (var item in arr.EnumerateArray())
                {
                    var m = JsonSerializer.Deserialize<Mobilier>(item.GetRawText(), opts);
                    if (m != null) mobiliers.Add(m);
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "MobilierDispatcher: échec parsing data.mobiliers. EventId={Id}", ev.Id);
                return;
            }

            if (mobiliers.Count == 0)
            {
                _logger.LogInformation("MobilierDispatcher: aucun mobilier à router. EventId={Id}", ev.Id);
                return;
            }

            // 2) Paramètres chunking
            int parentsPerBatch = int.TryParse(Environment.GetEnvironmentVariable("ParentsPerBatch"), out var n)
                                  ? Math.Max(1, n) : 100;
            int maxRequestsPerBatch = int.TryParse(Environment.GetEnvironmentVariable("MaxRequestsPerBatch"), out var r)
                                  ? Math.Min(Math.Max(10, r), 1000) : 1000;

            var queueName = Environment.GetEnvironmentVariable("MobilierQueueName") ?? "mobilier";
            await using var sender = _senderFactory.CreateSender(queueName);

            // 3) Découpage et envoi
            int i = 0;
            while (i < mobiliers.Count)
            {
                int requests = 0;
                var chunk = new List<Mobilier>();
                while (i < mobiliers.Count && chunk.Count < parentsPerBatch)
                {
                    // ici: 1 mobilier = 1 requête
                    if (requests + 1 > maxRequestsPerBatch && chunk.Count > 0) break;
                    chunk.Add(mobiliers[i]);
                    requests += 1;
                    i++;
                }

                var payload = new MobilierPayload
                {
                    EventType = ev.EventType ?? "mobilier.created",
                    Mobiliers = chunk
                };

                string messageId = BuildDeterministicMessageId(ev.Id, payload.EventType, chunk);
                var body = JsonSerializer.Serialize(payload);
                var msg = new ServiceBusMessage(body)
                {
                    MessageId = messageId,
                    CorrelationId = ev.Id,
                    ContentType = "application/json"
                };

                try
                {
                    await sender.SendMessageAsync(msg);
                    _logger.LogInformation(
                        "MobilierDispatcher: envoi OK | Queue={Queue} Parents={Count} Requests={Req} MsgId={MsgId}",
                        queueName, chunk.Count, requests, messageId);
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "MobilierDispatcher: envoi FAILED | Queue={Queue} MsgId={MsgId}",
                        queueName, messageId);
                    throw; // laisser Event Grid gérer le retry
                }
            }

            _logger.LogInformation("MobilierDispatcher done | EventId={Id}", ev.Id);
        }

        private static string BuildDeterministicMessageId(string eventId, string? eventType, List<Mobilier> items)
        {
            var ids = items
                .Select(m => m.IdStructure.ToString())
                .Where(s => !string.IsNullOrWhiteSpace(s))
                .OrderBy(s => s, StringComparer.Ordinal)
                .ToArray();

            var raw = $"{eventId}\n{eventType}\n{string.Join(",", ids)}";
            using var sha = SHA256.Create();
            return Convert.ToHexString(sha.ComputeHash(Encoding.UTF8.GetBytes(raw)));
        }
    }
}