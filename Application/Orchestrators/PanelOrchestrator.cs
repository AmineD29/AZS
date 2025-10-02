using Microsoft.Extensions.Logging;
using System.Text.Json;
using WorkOrderFunctions.Application.Orchestrators;
using WorkOrderFunctions.Config;
using WorkOrderFunctions.Domain.DTOs;
using WorkOrderFunctions.Infrastructure.Auth;
using WorkOrderFunctions.Infrastructure.Caching;
using WorkOrderFunctions.Infrastructure.Dataverse;
using WorkOrderFunctions.Infrastructure.Resilience;
using WorkOrderFunctions.Policies;
internal sealed class PanelBatchContext
{
    public List<(Dictionary<string, object> Payload, string EntitySetName, int ParentContentId)> Operations { get; } =
        new();

    public HashSet<string> PanelIds { get; } = new(); public HashSet<string> AccessTypeIds { get; } = new(); public HashSet<string> ProductIds { get; } = new(); public HashSet<string> SupportIds { get; } = new(); public HashSet<string> SurfaceIds { get; } = new();
    public HashSet<string> CategoryIds { get; } = new(); public HashSet<string> TerritoryIds { get; } = new(); public HashSet<string> AccountStructIds { get; } = new();
}

public class PanelOrchestrator : IPanelOrchestrator
{
    private readonly ITokenProvider _tokenProvider;
    private readonly IGuidCacheService _guidCache;
    private readonly IDataverseBatchClient _batchClient;
    private readonly IBatchRetryPolicy _retryPolicy;
    private readonly IGlobalConcurrencyGate _globalGate;
    private readonly IDataverseQueryClient _queryClient;
    private readonly IRedisCache _redis;
    private readonly AppSettings _settings;
    private readonly IPanelValidator _validator;

    public PanelOrchestrator(
        ITokenProvider tokenProvider,
        IGuidCacheService guidCache,
        IDataverseBatchClient batchClient,
        IBatchRetryPolicy retryPolicy,
        IGlobalConcurrencyGate globalGate,
        IDataverseQueryClient queryClient,
        IRedisCache redis,
        AppSettings settings,
        IPanelValidator validator)
    {
        _tokenProvider = tokenProvider;
        _guidCache = guidCache;
        _batchClient = batchClient;
        _retryPolicy = retryPolicy;
        _globalGate = globalGate;
        _queryClient = queryClient;
        _redis = redis;
        _settings = settings;
        _validator = validator;
    }

    public async Task ProcessAsync(string queueMessage, ILogger logger)
    {
        var (payload, _) = DeserializePayload(queueMessage, logger);
        var createOnly = IsCreateOnly(payload?.EventType);
        await ProcessAsync(queueMessage, logger, false);
    }

    public async Task ProcessAsync(string queueMessage, ILogger logger, bool createOnly)
    {
        var (payload, parseOk) = DeserializePayload(queueMessage, logger);
        if (!parseOk || payload is null || payload.Panels is null || payload.Panels.Count == 0)
        {
            throw new InvalidOperationException("PanelOrchestrator: payload vide ou invalide.");

        }

        var token = await _tokenProvider.GetAccessTokenAsync();
        var preload = await PreloadPanelGuidsAsync(payload, token, logger);
        var ctx = await BuildOperationsAsync(payload, preload, logger);
        await ExecuteLaterAsync(payload, ctx, token, createOnly, logger);
    }

    private static (PanelPayload? Payload, bool Ok) DeserializePayload(string json, ILogger logger)
    {
        try
        {
            var p = JsonSerializer.Deserialize<PanelPayload>(
                json,
                new JsonSerializerOptions { PropertyNameCaseInsensitive = true });

            if (p is null)
                throw new InvalidOperationException("PanelOrchestrator: payload null après désérialisation.");
            return (p, true);

        }
        catch (Exception ex)
        {

            logger.LogError(ex, "PanelOrchestrator: JSON invalide.");
            throw new InvalidOperationException("PanelOrchestrator: JSON invalide.", ex);

        }
    }

    private static bool IsCreateOnly(string? eventType)
        => (eventType ?? string.Empty).Equals("panel.created", StringComparison.OrdinalIgnoreCase);

    private static void CleanNullsAndEmptyStrings(IDictionary<string, object?> dict)
    {
        bool dropEmpty = !string.Equals(
            Environment.GetEnvironmentVariable("SendEmptyStringLabels"),
            "true", StringComparison.OrdinalIgnoreCase);

        var toRemove = new List<string>();
        foreach (var kv in dict)
        {
            if (kv.Value is null) { toRemove.Add(kv.Key); continue; }
            if (dropEmpty && kv.Value is string s && string.IsNullOrWhiteSpace(s))
                toRemove.Add(kv.Key);
        }
        foreach (var k in toRemove) dict.Remove(k);
    }

    private async Task<IDictionary<string, string>> PreloadPanelGuidsAsync(PanelPayload payload, string token, ILogger logger)
    {
        if (_guidCache is IGuidCacheService ex)
        {
            return await ex.PreloadPanelsAsync(payload, token, logger);
        }

        logger.LogWarning("GuidCacheServiceEx non disponible : aucun prchargement GUID effectu.");
        return new Dictionary<string, string>();
    }

    private async Task<PanelBatchContext> BuildOperationsAsync(
        PanelPayload payload,
        IDictionary<string, string> preload,
        ILogger logger)
    {
        var ctx = new PanelBatchContext();

        var accessTypeIds = payload.Panels.Select(p => p.IdTypeAcces.ToString()).Where(s => !string.IsNullOrWhiteSpace(s)).Distinct(StringComparer.OrdinalIgnoreCase);
        var categoryIds = payload.Panels.Select(p => p.IdTypePanel.ToString()).Where(s => !string.IsNullOrWhiteSpace(s)).Distinct(StringComparer.OrdinalIgnoreCase);
        var surfaceIds = payload.Panels.Select(p => p.IdArea.ToString()).Where(s => !string.IsNullOrWhiteSpace(s)).Distinct(StringComparer.OrdinalIgnoreCase);
        var productIds = payload.Panels.Where(p => p.IdProduct.HasValue).Select(p => p.IdProduct!.Value.ToString()).Distinct(StringComparer.OrdinalIgnoreCase);
        var supportIds = payload.Panels.Where(p => p.IdSupport.HasValue).Select(p => p.IdSupport!.Value.ToString()).Distinct(StringComparer.OrdinalIgnoreCase);

        foreach (var id in accessTypeIds)
        {
            var libelle = payload.Panels.FirstOrDefault(p => p.IdTypeAcces.ToString() == id)?.TypeAcces ?? string.Empty;
            ctx.AccessTypeIds.Add(id);
            await AddMetadataIfNotExistsAsync("dc_typedacceses", "dc_idtypedacces", id, libelle, ctx, preload, logger);
        }

        foreach (var id in categoryIds)
        {
            var desc = payload.Panels.FirstOrDefault(p => p.IdTypePanel.ToString() == id)?.TypePanel ?? string.Empty;
            ctx.CategoryIds.Add(id);
            await AddMetadataIfNotExistsAsync("msdyn_customerassetcategories", "msdyn_name", id, desc, ctx, preload, logger);
        }

        foreach (var id in surfaceIds)
        {
            var lib = payload.Panels.FirstOrDefault(p => p.IdArea.ToString() == id)?.Area ?? string.Empty;
            ctx.SurfaceIds.Add(id);
            await AddMetadataIfNotExistsAsync("dc_surfaces", "dc_idsurface", id, lib, ctx, preload, logger);
        }

        foreach (var id in productIds)
        {
            var lib = payload.Panels.FirstOrDefault(p => p.IdProduct?.ToString() == id)?.Product ?? string.Empty;
            ctx.ProductIds.Add(id);
            await AddMetadataIfNotExistsAsync("dc_produits", "dc_idproduct", id, lib, ctx, preload, logger);
        }

        foreach (var id in supportIds)
        {
            var lib = payload.Panels.FirstOrDefault(p => p.IdSupport?.ToString() == id)?.Support ?? string.Empty;
            ctx.SupportIds.Add(id);
            await AddMetadataIfNotExistsAsync("dc_supports", "dc_idsupport", id, lib, ctx, preload, logger);
        }

        var panelAltKey = Environment.GetEnvironmentVariable("AltKey_msdyn_customerassets") ?? "dc_idpanel";

        foreach (var p in payload.Panels)
        {
            if (!_validator.IsValid(p)) continue;
            logger.LogWarning($"Nbr panels : {payload.Panels.Count()}");
            var idPanelStr = p.IdPanel.ToString();
            string? accountGuid = TryGet(preload, $"accounts:dc_idstructure:{p.IdStructure}");
            string? categoryGuid = TryGet(preload, $"msdyn_customerassetcategories:msdyn_name:{p.IdTypePanel}");
            string? typeAccesGuid = TryGet(preload, $"dc_typedacceses:dc_idtypedacces:{p.IdTypeAcces}");
            string? surfaceGuid = TryGet(preload, $"dc_surfaces:dc_idsurface:{p.IdArea}");
            string? productGuid = p.IdProduct.HasValue ? TryGet(preload, $"dc_produits:dc_idproduct:{p.IdProduct.Value}") : null;
            string? supportGuid = p.IdSupport.HasValue ? TryGet(preload, $"dc_supports:dc_idsupport:{p.IdSupport.Value}") : null;
            string? territoryGuid = p.IdSite.HasValue ? TryGet(preload, $"territories:dc_idsite:{p.IdSite.Value}") : null;

            if (string.IsNullOrEmpty(accountGuid)) continue;

            int? stateCode = null;
            if (!string.IsNullOrWhiteSpace(p.Status) && (p.Status == "0" || p.Status == "1"))
                stateCode = int.Parse(p.Status);


            var payloadPanel = new Dictionary<string, object?>
            {
                [panelAltKey] = p.IdPanel,
                ["msdyn_name"] = p.RefPanel,
                ["statecode"] = stateCode,
                ["msdyn_account@odata.bind"] = $"/accounts({accountGuid})"
            };

            if (!string.IsNullOrEmpty(categoryGuid))
                payloadPanel["msdyn_CustomerAssetCategory@odata.bind"] = $"/msdyn_customerassetcategories({categoryGuid})";
            if (!string.IsNullOrEmpty(typeAccesGuid))
                payloadPanel["dc_Typedacces@odata.bind"] = $"/dc_typedacceses({typeAccesGuid})";
            if (!string.IsNullOrEmpty(surfaceGuid))
                payloadPanel["dc_Surface@odata.bind"] = $"/dc_surfaces({surfaceGuid})";
            if (!string.IsNullOrEmpty(productGuid))
                payloadPanel["dc_idproduit@odata.bind"] = $"/dc_produits({productGuid})";
            if (!string.IsNullOrEmpty(territoryGuid))
                payloadPanel["dc_serviceterritory@odata.bind"] = $"/territories({territoryGuid})";
            if (!string.IsNullOrEmpty(supportGuid))
                payloadPanel["dc_idsupport@odata.bind"] = $"/dc_supports({supportGuid})";

            payloadPanel["dc_idstartday"] = p.IdStartDay;
            int positioning = int.TryParse(p.Positioning, out var temp) && (temp == 0 || temp == 1) ? temp: -1;
            if (positioning != -1)
            {
                payloadPanel["dc_positioning"] = positioning;
            }
            payloadPanel["dc_libelletypedacces"] = p.TypeAcces;
            payloadPanel["dc_libelletypepanel"] = p.TypePanel;
            payloadPanel["dc_libellesurface"] = p.Area;
            payloadPanel["dc_product"] = p.Product;
            payloadPanel["dc_support"] = p.Support;

            CleanNullsAndEmptyStrings(payloadPanel);
            foreach (var kv in payloadPanel)
            {
                logger.LogWarning($"Operation : {kv.Key}, {kv.Value}");
            }
            if (ctx.PanelIds.Contains(idPanelStr)) continue;
            ctx.PanelIds.Add(idPanelStr);
            ctx.Operations.Add((payloadPanel, "msdyn_customerassets", 0));
        }

        if (ctx.Operations.Count == 0)
            throw new InvalidOperationException("PanelOrchestrator: aucune opération Dataverse construite (validations/lookups manquants).");
        return ctx;

    }

    private static string? TryGet(IDictionary<string, string> dict, string key)
        => dict.TryGetValue(key, out var v) ? v : null;

    private async Task AddMetadataIfNotExistsAsync(
        string entitySet,
        string keyField,
        string keyValue,
        string label,
        PanelBatchContext ctx,
        IDictionary<string, string> preload,
        ILogger logger)
    {
        string cacheKey = $"{entitySet}:{keyField}:{keyValue}";
        logger.LogInformation($"[Cache HIT VREIF] {cacheKey}");
        if (preload.ContainsKey(cacheKey))
        {
            logger.LogInformation($"[Cache HIT] {cacheKey} = {preload[cacheKey]}");
            return;
        }

        bool existsInBatch = ctx.Operations.Any(op =>
    op.EntitySetName.Equals(entitySet, StringComparison.OrdinalIgnoreCase) &&
    op.Payload.TryGetValue(keyField, out var v) &&
    v?.ToString() == keyValue);

        if (existsInBatch) return;

        var payload = new Dictionary<string, object?>
        {
            [keyField] = keyValue
        };

        switch (entitySet)
        {
            case "dc_typedacceses":
                payload["dc_libelletypedacces"] = label;
                break;
            case "msdyn_customerassetcategories":
                payload["dc_description"] = label;
                break;
            case "dc_surfaces":
                payload["dc_libellesurface"] = label;
                break;
            case "dc_produits":
                payload["dc_product"] = label;
                break;
            case "dc_supports":
                payload["dc_support"] = label;
                break;
            default:
                if (!string.IsNullOrWhiteSpace(label))
                    payload[$"dc_libelle{keyField.Substring(3).ToLower()}"] = label;
                break;
        }

        ctx.Operations.Add((payload, entitySet, 0));
    }

    private async Task ExecuteLaterAsync(
        PanelPayload payload,
        PanelBatchContext ctx,
        string token,
        bool createOnly,
        ILogger logger)
    {
        await _globalGate.WaitAsync();
        try
        {
            logger.LogInformation("Exécution du batch : {Count} opérations.", ctx.Operations.Count);
            var result = await _retryPolicy.ExecuteAsync(
                executeBatch: ops => _batchClient.ExecuteBatchAsync(ops, token, logger, createOnly),
                operations: ctx.Operations,
                logger: logger
            );

            if (!result.IsSuccess)
            {
                logger.LogError("Batch échoué ou partiel. IsThrottled={Throttled}, Error={Err}",
                    result.IsThrottled, result.ErrorMessage);

                throw new ApplicationException(
                $"Dataverse batch failed. Throttled={result.IsThrottled}, Error={result.ErrorMessage ?? "n/a"}");

            }
        }
        finally
        {
            _globalGate.Release();
        }

        var ttl = TimeSpan.FromDays(90);

        async Task UpdateRedisAsync(string entitySet, string keyField, string valueField, IEnumerable<string> ids)
        {
            if (!ids.Any()) return;
            var filter = string.Join(" or ", ids.Select(id => $"{keyField} eq '{id.Replace("'", "''")}'"));
            if (entitySet.Equals("msdyn_customerassets"))
            {
                filter = string.Join(" or ", ids.Select(id => $"{keyField} eq {id}"));
            }
            var url = $"{_settings.ResourceUrl}/api/data/v9.2/{entitySet}?$select={valueField},{keyField}&$filter={filter}";
            var map = await _queryClient.BulkQueryAndMapAsync(url, keyField, valueField, token, logger);
            foreach (var kv in map)
                await _redis.SetStringAsync($"{entitySet}:{keyField}:{kv.Key}", kv.Value, ttl);
        }

        await UpdateRedisAsync("msdyn_customerassets", "dc_idpanel", "msdyn_customerassetid", ctx.PanelIds);
        await UpdateRedisAsync("dc_typedacceses", "dc_idtypedacces", "dc_typedaccesid", ctx.AccessTypeIds);
        await UpdateRedisAsync("dc_produits", "dc_idproduct", "dc_produitid", ctx.ProductIds);
        await UpdateRedisAsync("dc_supports", "dc_idsupport", "dc_supportid", ctx.SupportIds);
        await UpdateRedisAsync("dc_surfaces", "dc_idsurface", "dc_surfaceid", ctx.SurfaceIds);
        await UpdateRedisAsync("msdyn_customerassetcategories", "msdyn_name", "msdyn_customerassetcategoryid", ctx.CategoryIds);

        logger.LogInformation("PanelOrchestrator: cache Redis rempli (90 jours) pour {Panels} panels, {Acc} types d'accès, {Prod} produits, {Supp} supports, {Surf} surfaces, {Cat} categories.",
            ctx.PanelIds.Count, ctx.AccessTypeIds.Count, ctx.ProductIds.Count, ctx.SupportIds.Count, ctx.SurfaceIds.Count, ctx.CategoryIds.Count);
    }
}
