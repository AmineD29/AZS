using Microsoft.Extensions.Logging;
using System.Text.Json;
using WorkOrderFunctions.Config;
using WorkOrderFunctions.Domain.DTOs;
using WorkOrderFunctions.Domain.Entities;
using WorkOrderFunctions.Infrastructure.Auth;
using WorkOrderFunctions.Infrastructure.Caching;
using WorkOrderFunctions.Infrastructure.Dataverse;
using WorkOrderFunctions.Infrastructure.Resilience;
using WorkOrderFunctions.Policies;
using WorkOrderFunctions.Services;

namespace WorkOrderFunctions.Application.Orchestrators
{
    public class WorkOrderOrchestrator : IWorkOrderOrchestrator
    {
        private readonly ITokenProvider _tokenProvider;
        private readonly IGuidCacheService _guidCache;
        private readonly IDataverseBatchClient _batchClient;
        private readonly IBatchRetryPolicy _retryPolicy;
        private readonly IWorkOrderValidator _validator;
        private readonly IGlobalConcurrencyGate _globalGate;
        private readonly IRedisCache _redis;
        private readonly AppSettings _settings;

        public WorkOrderOrchestrator(
            ITokenProvider tokenProvider,
            IGuidCacheService guidCache,
            IDataverseBatchClient batchClient,
            IBatchRetryPolicy retryPolicy,
            IWorkOrderValidator validator,
            IGlobalConcurrencyGate globalGate,
            AppSettings settings,
            IRedisCache redis)
        {
            _tokenProvider = tokenProvider;
            _guidCache = guidCache;
            _batchClient = batchClient;
            _retryPolicy = retryPolicy;
            _validator = validator;
            _globalGate = globalGate;
            _settings = settings;
            _redis = redis;
        }
        public List<(Dictionary<string, object> Payload, string EntitySetName, int ParentContentId)> Operations { get; } =
            new();

        public async Task ProcessAsync(string queueMessage, ILogger logger)
        {
            var (payload, _) = DeserializePayload(queueMessage, logger);
            var createOnly = IsCreateOnly(payload?.EventType);
            await ProcessAsync(queueMessage, logger, false);
        }
        private static bool IsCreateOnly(string? eventType)
            => (eventType ?? string.Empty).Equals("workorder.created", StringComparison.OrdinalIgnoreCase);
        private static (WorkOrderPayload? Payload, bool Ok) DeserializePayload(string json, ILogger logger)
        {

            try
            {
                var p = JsonSerializer.Deserialize<WorkOrderPayload>(
                json,
                new JsonSerializerOptions { PropertyNameCaseInsensitive = true });

                if (p is null)
                    throw new InvalidOperationException("WorkOrderOrchestrator: payload null après désérialisation.");

                return (p, true);
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "WorkOrderOrchestrator: JSON invalide.");
                throw new InvalidOperationException("WorkOrderOrchestrator: JSON invalide.", ex);
            }

        }
        public async Task ProcessAsync(string queueMessage, ILogger logger, bool createOnly)
        {
            var (payload, parseOk) = DeserializePayload(queueMessage, logger);
            if (!parseOk || payload is null || payload.WorkOrders is null || payload.WorkOrders.Count == 0)
            {
                throw new InvalidOperationException("WorkOrderOrchestrator: payload vide ou invalide.");
                return;
            }

            var token = await _tokenProvider.GetAccessTokenAsync();
            var preload = await _guidCache.PreloadAsync(payload, token, logger);

            int maxRequestsPerBatch = int.TryParse(Environment.GetEnvironmentVariable("MaxRequestsPerBatch"), out var r)
                ? Math.Min(Math.Max(10, r), 1000)
                : 1000;

            int index = 0;
            var parents = payload.WorkOrders ?? new List<WorkOrder>();

            string altKeyAttr = Environment.GetEnvironmentVariable("AltKey_msdyn_workorders");
            if (string.IsNullOrWhiteSpace(altKeyAttr)) altKeyAttr = "dc_name";

            while (index < parents.Count)
            {
                int requests = 0;
                var group = new List<WorkOrder>();
                while (index < parents.Count)
                {
                    var p = parents[index];
                    int reqForP = 1 + (p.SubWorkOrders?.Count ?? 0);
                    if (requests + reqForP > maxRequestsPerBatch && group.Count > 0)
                        break;
                    requests += reqForP;
                    group.Add(p);
                    index++;
                }

                await ProcessGroupAsync(group, preload, token, logger, createOnly, altKeyAttr);
            }
        }

        private async Task ProcessGroupAsync(
            List<WorkOrder> group,
            IDictionary<string, string> preload,
            string token,
            ILogger logger,
            bool createOnly,
            string altKeyAttr)
        {
            var operations = new List<(Dictionary<string, object>, string, int)>();
            var parentContentIdMap = new Dictionary<string, int>();
            var parentStructureGuidMap = new Dictionary<string, string>();
            int contentId = 1;

            async Task<string?> ResolveFromCaches(string cacheKey)
            {
                if (preload.TryGetValue(cacheKey, out var v1)) return v1;
                var v2 = _guidCache.TryGet(cacheKey);
                if (!string.IsNullOrEmpty(v2)) return v2;
                return await _redis.GetStringAsync(cacheKey);
            }


            foreach (var parent in group)
            {
                if (!_validator.IsValidWorkOrder(parent))
                {
                    continue;
                }

                var structureGuid = await ResolveFromCaches($"accounts:dc_idstructure:{parent.IdStructure}");
                var worktypeGuid = await ResolveFromCaches($"msdyn_workordertypes:msdyn_name:{parent.OtType}");
                var resourceGuid = await ResolveFromCaches($"bookableresources:dc_idressource:{parent.ResourceId}");

                if (structureGuid is null || worktypeGuid is null || resourceGuid is null)
                {
                    logger.LogError("Lookup manquant (Account/WorkType/Resource) pour parent {Id}. structure={S}, type={T}, res={R}",
                        parent.WorkOrderBroadcastId, structureGuid, worktypeGuid, resourceGuid);
                    continue;
                }
                var allPanelIds = group
                    .SelectMany(parent => parent.Panel ?? Enumerable.Empty<string>())
                    .Distinct()
                    .ToList();

                var panelNames = new Dictionary<string, string>();
                if (allPanelIds.Count > 0)
                {
                    try
                    {
                        var filter = string.Join(" or ", allPanelIds.Select(id => $"dc_idpanel eq {int.Parse(id)}"));
                        var panelQueryUrl = $"{_settings.ResourceUrl}/api/data/v9.0/msdyn_customerassets?$select=dc_idpanel,msdyn_name&$filter={filter}";

                        using var httpClient = new HttpClient();
                        httpClient.DefaultRequestHeaders.Authorization = new System.Net.Http.Headers.AuthenticationHeaderValue("Bearer", token);
                        var panelResponse = await httpClient.GetAsync(panelQueryUrl);

                        if (panelResponse.IsSuccessStatusCode)
                        {
                            var panelJson = await panelResponse.Content.ReadAsStringAsync();
                            using var panelDoc = JsonDocument.Parse(panelJson);
                            foreach (var panelElem in panelDoc.RootElement.GetProperty("value").EnumerateArray())
                            {
                                var idPanel = panelElem.GetProperty("dc_idpanel").GetInt32().ToString();
                                var name = panelElem.GetProperty("msdyn_name").GetString();
                                panelNames[idPanel] = name;
                            }
                        }
                        else
                        {
                            logger.LogError("Impossible de batch-récupérer les références panels. StatusCode={StatusCode}", panelResponse.StatusCode);
                        }
                    }
                    catch (Exception ex)
                    {
                        logger.LogError(ex.Message, "Erreur lors de la récupération batch des références panels.");
                    }
                }
                var panelRefs = new List<string>();
                foreach (var panelId in parent.Panel ?? Enumerable.Empty<string>())
                {
                    if (panelNames.TryGetValue(panelId, out var refPanel))
                        panelRefs.Add(refPanel);
                }
                if (panelRefs.Count == 0)
                    continue;

                string dcPanelValue = string.Join(",", panelRefs);

                var parentIdBiz = parent.WorkOrderBroadcastId ?? string.Empty;
                string defaulIncidentGUID = Environment.GetEnvironmentVariable("default_incident_type_guid");
                var parentDict = new Dictionary<string, object>
                {
                    { "msdyn_serviceaccount@odata.bind", $"/accounts({structureGuid})" },
                    { "msdyn_workordertype@odata.bind",  $"/msdyn_workordertypes({worktypeGuid})" },
                    { "msdyn_preferredresource@odata.bind", $"/bookableresources({resourceGuid})" },
                    { "dc_interne", parent.IsInternal },
                    { "dc_panel", dcPanelValue },
                    { altKeyAttr, parentIdBiz },
                    { "msdyn_autonumbering", parentIdBiz },
                    { "msdyn_primaryincidenttype@odata.bind", $"/msdyn_incidenttypes({defaulIncidentGUID})" }
                };

                parentContentIdMap[parent.WorkOrderBroadcastId] = contentId;
                parentStructureGuidMap[parent.WorkOrderBroadcastId] = structureGuid;

                operations.Add((parentDict, "msdyn_workorders", 0));
                contentId++;
            }

            foreach (var parent in group)
            {
                if (!parentContentIdMap.TryGetValue(parent.WorkOrderBroadcastId, out var _))
                    continue;

                var structureGuid = parentStructureGuidMap[parent.WorkOrderBroadcastId];

                var parentKeyRaw = parent.WorkOrderBroadcastId ?? string.Empty;
                var odataSafe = parentKeyRaw.Replace("'", "''");
                var pathValue = Uri.EscapeDataString(odataSafe);
                var parentAltKeyBind = $"/msdyn_workorders({altKeyAttr}='{pathValue}')";

                foreach (var child in parent.SubWorkOrders ?? Enumerable.Empty<SubWorkOrder>())
                {
                    if (!_validator.IsValidSubWorkOrder(child))
                        continue;

                    string defaulIncidentGUID = Environment.GetEnvironmentVariable("default_incident_type_guid");
                    var childTypeGuid = await ResolveFromCaches($"msdyn_workordertypes:msdyn_name:{child.Type}");
                    var assetPanelGuid = await ResolveFromCaches($"msdyn_customerassets:dc_idpanel:{child.IdPanel}");
                    var assetFaceGuid = await ResolveFromCaches($"msdyn_customerassets:dc_idface:{child.IdFace}");

                    var childIdBiz = child.SubWorkOrderId ?? string.Empty;

                    var childDict = new Dictionary<string, object?>
                    {
                        { altKeyAttr, childIdBiz },
                        { "msdyn_autonumbering", childIdBiz },
                        { "msdyn_workordertype@odata.bind", childTypeGuid != null ? $"/msdyn_workordertypes({childTypeGuid})" : null },
                        { "msdyn_customerasset@odata.bind", assetPanelGuid != null ? $"/msdyn_customerassets({assetPanelGuid})" : null },
                        { "dc_Idface@odata.bind", assetFaceGuid != null ? $"/msdyn_customerassets({assetFaceGuid})" : null },
                        { "msdyn_serviceaccount@odata.bind", $"/accounts({structureGuid})" },
                        { "msdyn_parentworkorder_msdyn_workorder@odata.bind", parentAltKeyBind },
                        { "msdyn_primaryincidenttype@odata.bind", $"/msdyn_incidenttypes({defaulIncidentGUID})" },
                        { "dc_comment", child.Comment },
                        { "dc_themedelordredetravail", child.Theme},
                        { "dc_bandeau", child.Bandeau},
                        { "dc_flap", child.Flap},
                        {"dc_isaudited",child.IsAudited },
                        {"dc_issuspended",child.IsSuspended },
                        {"dc_iskeepedinplace",child.IsKeepedInPlace },
                        {"dc_isleftinplace",child.IsLeftInPlace },
                        {"dc_ismandatoryphoto",child.IsMandatoryPhoto },
                        {"dc_libelleannonceur",child.AdvertiserLabel },
                        {"dc_managername", child.ManagerName },
                        {"dc_reason", child.Reason },
                        { "msdyn_systemstatus",child.IsCanceled ? "690970005" : null},
                        { "msdyn_timefrompromised", child.StartDate},
                        { "msdyn_timetopromised", child.EndDate},
                        { "msdyn_timewindowstart", child.StartHour},
                        { "msdyn_timewindowend", child.EndHour},


                   }
                ;

                    foreach (var k in childDict.Where(kv => kv.Value is null).Select(kv => kv.Key).ToList())
                        childDict.Remove(k);

                    operations.Add((childDict!, "msdyn_workorders", 0));
                }
            }



            if (operations.Count == 0)
            {
                throw new InvalidOperationException("WorkOrderOrchestrator: aucune opération Dataverse construite (lookups/validations manquants).");
            }

            await _globalGate.WaitAsync();
            try
            {
                var result = await _retryPolicy.ExecuteAsync(
                    ops => _batchClient.ExecuteBatchAsync(ops, token, logger, createOnly),
                    operations,
                    logger);

                if (!result.IsSuccess)
                {
                    logger.LogError("Batch non terminé avec succès (IsThrottled={Throttled}). Error={Err}",
                        result.IsThrottled, result.ErrorMessage);
                    throw new ApplicationException(
                    $"Dataverse batch failed. Throttled={result.IsThrottled}, Error={result.ErrorMessage ?? "n/a"}");

                }
            }
            finally
            {
                _globalGate.Release();
            }
        }
    }
}
