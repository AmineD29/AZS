using System.Text.Json;
using System.Security.Cryptography;
using System.Text;
using Azure.Messaging.EventGrid;
using Azure.Messaging.ServiceBus;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;
using WorkOrderFunctions.Domain.DTOs;
using WorkOrderFunctions.Domain.Entities;
using WorkOrderFunctions.Infrastructure.Messaging;

namespace WorkOrderFunctions.Functions
{
    public class PanelDispatcherFunction
    {
        private readonly IServiceBusSenderFactory _senderFactory;
        private readonly ILogger<PanelDispatcherFunction> _logger;

        public PanelDispatcherFunction(IServiceBusSenderFactory senderFactory,
                                       ILogger<PanelDispatcherFunction> logger)
        {
            _senderFactory = senderFactory;
            _logger = logger;
        }

        [Function("PanelDispatcher")]
        public async Task Run([EventGridTrigger] EventGridEvent ev, FunctionContext ctx)
        {
            _logger.LogInformation("PanelDispatcher start | EventId={Id} Type={Type} Subject={Subject}",
                ev.Id, ev.EventType, ev.Subject);

            // 1) Extraire data.panels (tableau brut venant d’Event Grid)
            List<Panel> panels;
            try
            {
                using var doc = JsonDocument.Parse(ev.Data?.ToString() ?? "{}");
                if (!doc.RootElement.TryGetProperty("panels", out var arr) || arr.ValueKind != JsonValueKind.Array)
                {
                    _logger.LogWarning("PanelDispatcher: payload invalide (data.panels manquant). EventId={Id}", ev.Id);
                    return;
                }

                panels = new List<Panel>(capacity: arr.GetArrayLength());
                foreach (var item in arr.EnumerateArray())
                {
                    var p = JsonSerializer.Deserialize<Panel>(
                        item.GetRawText(),
                        new JsonSerializerOptions { PropertyNameCaseInsensitive = true });
                    if (p != null) panels.Add(p);
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "PanelDispatcher: échec de parsing data.panels. EventId={Id}", ev.Id);
                return;
            }

            if (panels.Count == 0)
            {
                _logger.LogInformation("PanelDispatcher: aucun panel à router. EventId={Id}", ev.Id);
                return;
            }

            // 2) Paramètres de découpe
            int parentsPerBatch = int.TryParse(Environment.GetEnvironmentVariable("ParentsPerBatch"), out var n)
                ? Math.Max(1, n) : 100;

            int maxRequestsPerBatch = int.TryParse(Environment.GetEnvironmentVariable("MaxRequestsPerBatch"), out var r)
                ? Math.Min(Math.Max(10, r), 1000) : 1000;

            var queueName = "panel"; // file unique; l’orchestrateur décidera createOnly via EventType
            await using var sender = _senderFactory.CreateSender(queueName);

            // 3) Découpage en lots et envoi
            int i = 0;
            while (i < panels.Count)
            {
                int requests = 0;
                var chunk = new List<Panel>();
                while (i < panels.Count && chunk.Count < parentsPerBatch)
                {
                    // 1 panel = 1 requête
                    if (requests + 1 > maxRequestsPerBatch && chunk.Count > 0) break;
                    chunk.Add(panels[i]);
                    requests += 1;
                    i++;
                }

                var payload = new PanelPayload
                {
                    EventType = ev.EventType ?? "panel.created",
                    Panels = chunk
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
                        "PanelDispatcher: envoi OK | Queue={Queue} Parents={Count} Requests={Req} MsgId={MsgId}",
                        queueName, chunk.Count, requests, messageId);
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "PanelDispatcher: envoi FAILED | Queue={Queue} MsgId={MsgId}", queueName, messageId);
                    throw; // laisser la plateforme gérer le retry Event Grid
                }
            }

            _logger.LogInformation("PanelDispatcher done | EventId={Id}", ev.Id);
        }

        private static string BuildDeterministicMessageId(string eventId, string? eventType, List<Panel> items)
        {
            var ids = items
                .Select(p => p.IdPanel.ToString())
                .Where(s => !string.IsNullOrWhiteSpace(s))
                .OrderBy(s => s, StringComparer.Ordinal)
                .ToArray();

            var raw = $"{eventId}\n{eventType}\n{string.Join(",", ids)}";
            using var sha = SHA256.Create();
            return Convert.ToHexString(sha.ComputeHash(Encoding.UTF8.GetBytes(raw)));
        }
    }
}
