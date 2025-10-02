using Microsoft.Extensions.Logging;
using System.Text.Json;
using WorkOrderFunctions.Config;
using WorkOrderFunctions.Domain.DTOs;
using WorkOrderFunctions.Infrastructure.Auth;
using WorkOrderFunctions.Infrastructure.Caching;
using WorkOrderFunctions.Infrastructure.Dataverse;
using WorkOrderFunctions.Infrastructure.Resilience;
using WorkOrderFunctions.Policies;
using WorkOrderFunctions.Services;

namespace WorkOrderFunctions.Application.Orchestrators;

internal sealed class MobilierBatchContext
{
    public List<(Dictionary<string, object> Payload, string EntitySetName, int ParentContentId)> Operations { get; } = new();
    public HashSet<string> GammeIds { get; } = new(StringComparer.OrdinalIgnoreCase);
    public HashSet<string> TerritoryIds { get; } = new(StringComparer.OrdinalIgnoreCase);
}

public sealed class MobilierOrchestrator : IMobilierOrchestrator
{
    private readonly ITokenProvider _tokenProvider;
    private readonly IGuidCacheService _guidCache;
    private readonly IDataverseBatchClient _batchClient;
    private readonly IBatchRetryPolicy _retryPolicy;
    private readonly IGlobalConcurrencyGate _globalGate;
    private readonly IDataverseQueryClient _queryClient;
    private readonly IRedisCache _redis;
    private readonly AppSettings _settings;
    private readonly IMobilierValidator _validator;

    public MobilierOrchestrator(
        ITokenProvider tokenProvider,
        IGuidCacheService guidCache,
        IDataverseBatchClient batchClient,
        IBatchRetryPolicy retryPolicy,
        IGlobalConcurrencyGate globalGate,
        IDataverseQueryClient queryClient,
        IRedisCache redis,
        AppSettings settings,
        IMobilierValidator validator)
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

        var (payload, ok) = DeserializePayload(queueMessage, logger);
        if (!ok || payload is null || payload.Mobiliers.Count == 0)
        {
            throw new InvalidOperationException("MobilierOrchestrator: payload vide ou invalide.");
        }
        var createOnly = IsCreateOnly(payload.EventType);
        await ProcessAsync(queueMessage, logger, false);
    }

    public async Task ProcessAsync(string queueMessage, ILogger logger, bool createOnly)
    {
        var (payload, ok) = DeserializePayload(queueMessage, logger);
        if (!ok || payload is null || payload.Mobiliers.Count == 0)
        {
            logger.LogWarning("MobilierOrchestrator: payload vide/invalide.");
            return;
        }

        var token = await _tokenProvider.GetAccessTokenAsync();

        var preload = await PreloadMobilierGuidsAsync(payload, token, logger);

        var metaCtx = await BuildMetadataOperationsAsync(payload, preload, logger);

        if (metaCtx.Operations.Count > 0)
        {
            logger.LogInformation("MobilierOrchestrator: exécution batch métadonnées ({Count} ops)…", metaCtx.Operations.Count);
            await ExecuteBatchAsync(metaCtx.Operations, token, createOnly: true, logger);
        }
        else
        {
            logger.LogInformation("MobilierOrchestrator: aucune métadonnée à créer (gammes existantes).");
        }


        var mainCtx = await BuildAccountOperationsAsync(payload, preload, logger);
        if (mainCtx.Operations.Count == 0)
        {
            logger.LogInformation("MobilierOrchestrator: aucun account à traiter.");
            return;
        }

        logger.LogInformation("MobilierOrchestrator: exécution batch accounts ({Count} ops)…", mainCtx.Operations.Count);
        await ExecuteBatchAsync(mainCtx.Operations, token, createOnly, logger);

        var altKeyAttr = Environment.GetEnvironmentVariable("AltKey_accounts ") ?? "name";
        await UpdateRedisAfterAccountsAsync(
            altKeyValues: payload.Mobiliers.Select(m =>
                altKeyAttr.Equals("dc_idstructure", StringComparison.OrdinalIgnoreCase)
                    ? (string.IsNullOrWhiteSpace(m.Ref) ? m.IdStructure.ToString() : m.Ref!)
                    : m.IdStructure.ToString()),
            altKeyAttr: altKeyAttr,
            token: token,
            logger: logger
        );

    }

    private static (MobilierPayload? Payload, bool Ok) DeserializePayload(string json, ILogger logger)
    {
        try
        {
            var p = JsonSerializer.Deserialize<MobilierPayload>(
                json,
                new JsonSerializerOptions { PropertyNameCaseInsensitive = true });

            if (p is null)
                throw new InvalidOperationException("MobilierOrchestrator: payload null après désérialisation.");
            return (p, true);

        }
        catch (Exception ex)
        {
            logger.LogError(ex, "MobilierOrchestrator: JSON invalide.");
            throw new InvalidOperationException("MobilierOrchestrator: JSON invalide.", ex);
        }
    }

    private static bool IsCreateOnly(string? eventType)
        => (eventType ?? string.Empty).Equals("mobilier.created", StringComparison.OrdinalIgnoreCase);

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

    private static string? TryGet(IDictionary<string, string> dict, string key)
        => dict.TryGetValue(key, out var v) ? v : null;

    private async Task<MobilierBatchContext> BuildAccountOperationsAsync(
        MobilierPayload payload,
        IDictionary<string, string> preload,
        ILogger logger)
    {
        var ctx = new MobilierBatchContext();

        var altKeyAttr = Environment.GetEnvironmentVariable("AltKey_accounts")
 ?? "name";

        var accountKeyField = Environment.GetEnvironmentVariable("AccountKeyField") ?? "name";
        var territoryKeyField = Environment.GetEnvironmentVariable("TerritoryKeyField") ?? "name";

        foreach (var m in payload.Mobiliers)
        {
            if (!_validator.IsValid(m)) continue;

            if (m.IdStructure <= 0 || string.IsNullOrWhiteSpace(m.Name))
                continue;


            string? territoireGuid = m.IdSite.HasValue
                ? TryGet(preload, $"territories:{territoryKeyField}:{m.IdSite.Value}")
                : null;

            string? gammeGuid = (m.IdGamme.HasValue && m.IdGamme.Value != 0)
                ? TryGet(preload, $"dc_gammes:dc_idgamme:{m.IdGamme.Value}")
                : null;


            int? stateCode = null;
            if (!string.IsNullOrWhiteSpace(m.Status) && (m.Status == "0" || m.Status == "1"))
                stateCode = int.Parse(m.Status);

            bool geocodedBySystem = (m.Latitude.GetValueOrDefault(0) == 0 &&
                                     m.Longitude.GetValueOrDefault(0) == 0);
            logger.LogInformation($"MobilierOrchestrator: Géocodage {geocodedBySystem} pour mobilier");

            var account = new Dictionary<string, object?>
            {
                [altKeyAttr] = m.IdStructure,
                ["name"] = m.IdStructure.ToString(),
                ["msdyn_workorderinstructions"] = m.Instructions,
                ["dc_idstructure"] = m.Ref,
                ["dc_nomdumobilier"] = m.Name,

                ["address1_line1"] = m.ServiceAddress?.Street,
                ["address1_line2"] = m.ServiceAddress?.Complement,
                ["address1_city"] = m.ServiceAddress?.City,
                ["address1_postalcode"] = m.ServiceAddress?.PostalCode,

                ["address2_line1"] = m.CommercialAddress?.Street,
                ["address2_city"] = m.CommercialAddress?.City,
                ["address2_postalcode"] = m.CommercialAddress?.PostalCode,

                ["dc_firsrtopeningstart"] = m.Preference?.FirstOpeningStart,
                ["dc_firsrtopeningend"] = m.Preference?.FirstOpeningEnd,
                ["dc_secondopeningstart"] = m.Preference?.SecondOpeningStart,
                ["dc_secondopeningend"] = m.Preference?.SecondOpeningEnd,

                ["statecode"] = stateCode,
                ["address1_latitude"] = m.Latitude == 0 ? null : m.Latitude,
                ["address1_longitude"] = m.Longitude == 0 ? null : m.Longitude,
                ["dc_geocodedbysystem"] = geocodedBySystem,
                ["address2_latitude"] = m.CommercialLatitude,
                ["address2_longitude"] = m.CommercialLongitude,
                ["dc_idgamme@odata.bind"] = $"/dc_gammes({gammeGuid})",
                ["dc_gamme"] = m.Gamme
            };

            if (!string.IsNullOrEmpty(territoireGuid))
                account["msdyn_serviceterritory@odata.bind"] = $"/territories({territoireGuid})";


            CleanNullsAndEmptyStrings(account);

            ctx.Operations.Add((new Dictionary<string, object>(account!), "accounts", 0));
        }

        if (ctx.Operations.Count == 0)
            throw new InvalidOperationException("MobilierOrchestrator: aucune opération Dataverse construite (validations/lookups manquants).");
        return ctx;

    }


    private async Task UpdateRedisAfterAccountsAsync(
        IEnumerable<string> altKeyValues,
        string altKeyAttr,
        string token,
        ILogger logger)
    {
        var ids = altKeyValues.Distinct(StringComparer.Ordinal).ToArray();
        if (ids.Length == 0) return;

        var keyField = altKeyAttr;
        var valueField = "accountid";
        var baseUrl = _settings.ResourceUrl;

        string ToFilterLiteral(string v)
    => long.TryParse(v, out _) && !keyField.Equals("name", StringComparison.OrdinalIgnoreCase)
       ? v
       : $"'{v.Replace("'", "''")}'";

        var filter = string.Join(" or ", ids.Select(v => $"{keyField} eq {ToFilterLiteral(v)}"));
        var url = $"{baseUrl}/api/data/v9.2/accounts?$select={valueField},{keyField}&$filter={filter}";

        var map = await _queryClient.BulkQueryAndMapAsync(url, keyField, valueField, token, logger);

        var ttl = TimeSpan.FromDays(90);
        foreach (var kv in map)
        {
            var cacheKey = $"accounts:dc_idstructure:{kv.Key}";
            await _redis.SetStringAsync(cacheKey, kv.Value, ttl);
            logger.LogInformation("MobilierOrchestrator: Redis set {cacheKey} => {Guid}", cacheKey, kv.Value);
        }

        logger.LogInformation("MobilierOrchestrator: Redis rempli pour {Count} accounts (clé={cacheKey}).",
            map.Count, keyField);
    }
    private async Task<MobilierBatchContext> BuildMetadataOperationsAsync(
    MobilierPayload payload,
    IDictionary<string, string> preload,
    ILogger logger)
    {
        var ctx = new MobilierBatchContext();

        foreach (var m in payload.Mobiliers)
        {
            if (!_validator.IsValid(m)) continue;

            if (m.IdGamme.HasValue && m.IdGamme.Value != 0)
            {
                var id = m.IdGamme.Value.ToString();
                var cacheKey = $"dc_gammes:dc_idgamme:{id}";
                var exists = preload.ContainsKey(cacheKey);
                if (!exists && !ctx.GammeIds.Contains(id))
                {
                    ctx.GammeIds.Add(id);
                    var payloadGamme = new Dictionary<string, object?>
                    {
                        ["dc_idgamme"] = id,
                        ["dc_gamme"] = m.Gamme ?? string.Empty
                    };
                    CleanNullsAndEmptyStrings(payloadGamme);
                    ctx.Operations.Add((payloadGamme!, "dc_gammes", 0));
                }
            }
        }

        return ctx;
    }

    private async Task ExecuteBatchAsync(
        List<(Dictionary<string, object> Payload, string EntitySetName, int ParentContentId)> operations,
        string token,
        bool createOnly,
        ILogger logger)
    {
        await _globalGate.WaitAsync();
        try
        {
            var result = await _retryPolicy.ExecuteAsync(
                executeBatch: ops => _batchClient.ExecuteBatchAsync(ops, token, logger, createOnly),
                operations: operations,
                logger: logger
            );

            if (!result.IsSuccess)
            {
                logger.LogError("Mobilier batch FAILED. Throttled={Throttled}, Error={Err}",
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

    private async Task<IDictionary<string, string>> PreloadMobilierGuidsAsync(
MobilierPayload payload, string token, ILogger logger)
    {
        return await _guidCache.PreloadMobiliersAsync(payload, token, logger);
    }

}
