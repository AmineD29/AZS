using Microsoft.Extensions.Logging;
using WorkOrderFunctions.Infrastructure.Dataverse;
using WorkOrderFunctions.Domain.DTOs;
using WorkOrderFunctions.Config;

namespace WorkOrderFunctions.Infrastructure.Caching;

public class GuidCacheService : IGuidCacheService
{
    private readonly IRedisCache _redis;
    private readonly IDataverseQueryClient _queryClient;
    private readonly AppSettings _settings;
    private readonly Dictionary<string, string> _local = new(StringComparer.OrdinalIgnoreCase);

    public GuidCacheService(IRedisCache redis, IDataverseQueryClient queryClient, AppSettings settings)
    {
        _redis = redis;
        _queryClient = queryClient;
        _settings = settings;
    }

    public string? TryGet(string key) => _local.TryGetValue(key, out var v) ? v : null;

    public async Task<IDictionary<string, string>> PreloadAsync(WorkOrderPayload payload, string token, ILogger logger)
    {
        var result = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        bool debug = string.Equals(Environment.GetEnvironmentVariable("CacheDebug"), "true", StringComparison.OrdinalIgnoreCase);

        // keys
        var accountKeyField  = Environment.GetEnvironmentVariable("AccountKeyField")  ?? "dc_idstructure";
        var resourceKeyField = Environment.GetEnvironmentVariable("ResourceKeyField") ?? "dc_idressource";
        var panelKeyField    = Environment.GetEnvironmentVariable("PanelKeyField")    ?? "dc_idpanel";
        var faceKeyField     = Environment.GetEnvironmentVariable("FaceKeyField")     ?? "dc_idface";

        var accounts  = payload.WorkOrders.Select(w => w.IdStructure.ToString()).Where(x => !string.IsNullOrWhiteSpace(x)).Distinct().ToList();
        var worktypes = payload.WorkOrders.Select(w => w.OtType).Where(x => !string.IsNullOrWhiteSpace(x)).Distinct().ToList();
        var resources = payload.WorkOrders.Select(w => w.ResourceId.ToString()).Where(x => !string.IsNullOrWhiteSpace(x)).Distinct().ToList();

        var children  = payload.WorkOrders.SelectMany(w => w.SubWorkOrders ?? Enumerable.Empty<Domain.Entities.SubWorkOrder>()).ToList();
        var childTypes= children.Select(c => c.Type).Where(x => !string.IsNullOrWhiteSpace(x)).Distinct().ToList();
        var panels    = children.Select(c => c.IdPanel).Where(x => !string.IsNullOrWhiteSpace(x)).Distinct().ToList();
        var faces     = children.Select(c => c.IdFace.ToString()).Where(x => !string.IsNullOrWhiteSpace(x)).Distinct().ToList();

        // Accounts
        await ResolveAndCacheAsync(
            ids: accounts,
            buildCacheKey: id => $"accounts:{accountKeyField}:{id}",
            buildUrlForMissing: missing => {
                var numeric = accountKeyField != "name" && missing.All(x => long.TryParse(x, out _));
                var filters = missing.Select(id => numeric ? $"name eq '{id}'" : $"name eq '{id}'");
                return $"{_settings.ResourceUrl}/api/data/v9.2/accounts?$select=accountid,{accountKeyField},name&$filter=" + string.Join(" or ", filters);
            },
            keyField: "name",
            idField: "accountid",
            token: token,
            logger: logger,
            result: result,
            debug: debug
        );

        // Work order types (parent + child types)
        var allTypes = worktypes.Concat(childTypes).Distinct().ToList();
        await ResolveAndCacheAsync(
            ids: allTypes,
            buildCacheKey: id => $"msdyn_workordertypes:msdyn_name:{id}",
            buildUrlForMissing: missing => $"{_settings.ResourceUrl}/api/data/v9.2/msdyn_workordertypes?$select=msdyn_name,msdyn_workordertypeid&$filter=" +
                                           string.Join(" or ", missing.Select(id => $"msdyn_name eq '{id.Replace("'","''")}'")),
            keyField: "msdyn_name",
            idField: "msdyn_workordertypeid",
            token: token,
            logger: logger,
            result: result,
            debug: debug
        );

        // Resources
        await ResolveAndCacheAsync(
            ids: resources,
            buildCacheKey: id => $"bookableresources:{resourceKeyField}:{id}",
            buildUrlForMissing: missing => {
                var numeric = resourceKeyField != "name" && missing.All(x => long.TryParse(x, out _));
                var filters = missing.Select(id =>
                    numeric
                        ? $"{resourceKeyField} eq {id}"        // pour les IDs purement numériques
                        : $"{resourceKeyField} eq '{id}'"      // pour les GUID ou strings → on met des quotes !
                );
            return $"{_settings.ResourceUrl}/api/data/v9.2/bookableresources" +
           "?$select=bookableresourceid,name," + resourceKeyField +
           "&$filter=" + string.Join(" or ", filters);
},
            keyField: resourceKeyField,
            idField: "bookableresourceid", 
            token: token,
            logger: logger,
            result: result,
            debug: debug
        );

        // Panels
        await ResolveAndCacheAsync(
            ids: panels,
            buildCacheKey: id => $"msdyn_customerassets:dc_idpanel:{id}",
            buildUrlForMissing: missing => $"{_settings.ResourceUrl}/api/data/v9.2/msdyn_customerassets?$select=msdyn_customerassetid,dc_idpanel&$filter=" +
                                           string.Join(" or ", missing.Select(id => $"dc_idpanel eq {id}")),
            keyField: "dc_idpanel",
            idField: "msdyn_customerassetid",
            token: token,
            logger: logger,
            result: result,
            debug: debug
        );

        // Faces
        await ResolveAndCacheAsync(
            ids: faces,
            buildCacheKey: id => $"msdyn_customerassets:{faceKeyField}:{id}",
            buildUrlForMissing: missing => $"{_settings.ResourceUrl}/api/data/v9.2/msdyn_customerassets?$select=msdyn_customerassetid,{faceKeyField}&$filter=" +
                                           string.Join(" or ", missing.Select(id => $"{faceKeyField} eq {id}")),
            keyField: faceKeyField,
            idField: "msdyn_customerassetid",
            token: token,
            logger: logger,
            result: result,
            debug: debug
        );

        logger.LogInformation("[Cache] Preload terminé : {Count} clés résolues (Redis/local).", result.Count);
        return result;
    }
    public async Task<IDictionary<string, string>> PreloadPanelsAsync(PanelPayload payload, string token, ILogger logger)
    {
        var result = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        bool debug = string.Equals(Environment.GetEnvironmentVariable("CacheDebug"), "true", StringComparison.OrdinalIgnoreCase);

        var accountKeyField = Environment.GetEnvironmentVariable("AccountKeyField") ?? "dc_idstructure";
        var territoryKeyField = Environment.GetEnvironmentVariable("TerritoryKeyField") ?? "dc_idsite";
        var baseUrl = _settings.ResourceUrl;

        var structures = payload.Panels.Select(p => p.IdStructure.ToString()).Where(x => !string.IsNullOrWhiteSpace(x)).Distinct().ToList();
        var typePanels = payload.Panels.Select(p => p.IdTypePanel.ToString()).Where(x => !string.IsNullOrWhiteSpace(x)).Distinct().ToList();
        var typeAcces = payload.Panels.Select(p => p.IdTypeAcces.ToString()).Where(x => !string.IsNullOrWhiteSpace(x)).Distinct().ToList();
        var products = payload.Panels.Where(p => p.IdProduct.HasValue).Select(p => p.IdProduct!.Value.ToString()).Distinct().ToList();
        var supports = payload.Panels.Where(p => p.IdSupport.HasValue).Select(p => p.IdSupport!.Value.ToString()).Distinct().ToList();
        var surfaces = payload.Panels.Select(p => p.IdArea.ToString()).Where(x => !string.IsNullOrWhiteSpace(x)).Distinct().ToList();
        var territories = payload.Panels.Where(p => p.IdSite.HasValue).Select(p => p.IdSite!.Value.ToString()).Distinct().ToList();

        // ---- Accounts ----
        await ResolveAndCacheAsync(
            ids: structures,
            buildCacheKey: id => $"accounts:{accountKeyField}:{id}",
            buildUrlForMissing: missing =>
            {
                var filters = missing.Select(id =>
                    (accountKeyField != "name" && long.TryParse(id, out _))
                        ? $"name eq '{id}'"
                        : $"name eq '{id.Replace("'", "''")}'");
                return $"{baseUrl}/api/data/v9.2/accounts" +
                       $"?$select=accountid,{accountKeyField},name&$filter=" + string.Join(" or ", filters);
            },
            keyField: "name",
            idField: "accountid",
            token, logger, result, debug
        );

        // ---- Catégories (type panel) ----
        await ResolveAndCacheAsync(
            ids: typePanels,
            buildCacheKey: id => $"msdyn_customerassetcategories:msdyn_name:{id}",
            buildUrlForMissing: missing =>
                $"{baseUrl}/api/data/v9.2/msdyn_customerassetcategories" +
                "?$select=msdyn_customerassetcategoryid,msdyn_name&$filter=" +
                string.Join(" or ", missing.Select(id => $"msdyn_name eq '{id.Replace("'", "''")}'")),
            keyField: "msdyn_name",
            idField: "msdyn_customerassetcategoryid",
            token, logger, result, debug
        );

        // ---- Produits ----
        await ResolveAndCacheAsync(
            ids: products,
            buildCacheKey: id => $"dc_produits:dc_idproduct:{id}",
            buildUrlForMissing: missing =>
                $"{baseUrl}/api/data/v9.2/dc_produits" +
                "?$select=dc_produitid,dc_idproduct&$filter=" +
                string.Join(" or ", missing.Select(id => $"dc_idproduct eq '{id.Replace("'", "''")}'")),
            keyField: "dc_idproduct",
            idField: "dc_produitid",
            token, logger, result, debug
        );

        // ---- Supports ----
        await ResolveAndCacheAsync(
            ids: supports,
            buildCacheKey: id => $"dc_supports:dc_idsupport:{id}",
            buildUrlForMissing: missing =>
                $"{baseUrl}/api/data/v9.2/dc_supports" +
                "?$select=dc_supportid,dc_idsupport&$filter=" +
                string.Join(" or ", missing.Select(id => $"dc_idsupport eq '{id.Replace("'", "''")}'")),
            keyField: "dc_idsupport",
            idField: "dc_supportid",
            token, logger, result, debug
        );

        // ---- Surfaces ----
        await ResolveAndCacheAsync(
            ids: surfaces,
            buildCacheKey: id => $"dc_surfaces:dc_idsurface:{id}",
            buildUrlForMissing: missing =>
                $"{baseUrl}/api/data/v9.2/dc_surfaces" +
                "?$select=dc_surfaceid,dc_idsurface&$filter=" +
                string.Join(" or ", missing.Select(id => $"dc_idsurface eq '{id.Replace("'", "''")}'")),
            keyField: "dc_idsurface",
            idField: "dc_surfaceid",
            token, logger, result, debug
        );
        // ---- Type d'accès ----
        await ResolveAndCacheAsync(
            ids: typeAcces,
            buildCacheKey: id => $"dc_typedacceses:dc_idtypedacces:{id}",
            buildUrlForMissing: missing =>
                $"{baseUrl}/api/data/v9.2/dc_typedacceses" +
                "?$select=dc_idtypedacces,dc_typedaccesid&$filter=" +
                string.Join(" or ", missing.Select(id => $"dc_idtypedacces eq '{id.Replace("'", "''")}'")),
            keyField: "dc_idtypedacces",
            idField: "dc_typedaccesid",
            token, logger, result, debug
        );

        // ---- Territories (site) ----
        await ResolveAndCacheAsync(
            ids: territories,
            buildCacheKey: id => $"territories:{territoryKeyField}:{id}",
            buildUrlForMissing: missing =>
            {
                var filters = missing.Select(id =>
                    (long.TryParse(id, out _))
                        ? $"name eq '{id}'"
                        : $"name eq '{id}'");
                return $"{baseUrl}/api/data/v9.2/territories" +
                       $"?$select=territoryid,name&$filter=" + string.Join(" or ", filters);
            },
            keyField: "name",
            idField: "territoryid",
            token, logger, result, debug
        );

        logger.LogInformation("[Cache] Preload Panels terminé : {Count} clés résolues (Redis/local).", result.Count);
        return result;
    }


    public async Task<IDictionary<string, string>> PreloadMobiliersAsync(
        MobilierPayload payload, string token, ILogger logger)
    {
        var result = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        bool debug = string.Equals(Environment.GetEnvironmentVariable("CacheDebug"), "true", StringComparison.OrdinalIgnoreCase);

        // Paramètres configurables (laisser par défaut alignés avec le flux “mobiler.cs”)
        var accountKeyField = Environment.GetEnvironmentVariable("AccountKeyField") ?? "name";   // alt-key pour accounts
        var territoryKeyField = Environment.GetEnvironmentVariable("TerritoryKeyField") ?? "name";   // lookup territories par name
        var baseUrl = _settings.ResourceUrl;

        // 1) Accounts (clé fonctionnelle : IdStructure ⇒ alt-key configurable)
        var accountIds = payload.Mobiliers
            .Select(m => m.IdStructure.ToString())
            .Where(s => !string.IsNullOrWhiteSpace(s))
            .Distinct()
            .ToList();

        await ResolveAndCacheAsync(
            ids: accountIds,
            buildCacheKey: id => $"accounts:{accountKeyField}:{id}",
            buildUrlForMissing: missing =>
            {
                // Filtrer sur accountKeyField (string-quote systématique pour éviter ambiguïtés)
                var filters = missing.Select(id => $"{accountKeyField} eq '{id.Replace("'", "''")}'");
                return $"{baseUrl}/api/data/v9.2/accounts?$select=accountid,{accountKeyField}&$filter=" + string.Join(" or ", filters);
            },
            keyField: accountKeyField,         // champ de clé (ex. name ou dc_idstructure)
            idField: "accountid",              // GUID à récupérer
            token: token,
            logger: logger,
            result: result,
            debug: debug
        );

        // 2) Territories (clé métier : idsite ⇒ lookup par 'name' côté Dataverse)
        var territoryIds = payload.Mobiliers
            .Where(m => m.IdSite.HasValue)
            .Select(m => m.IdSite!.Value.ToString())
            .Distinct()
            .ToList();

        await ResolveAndCacheAsync(
            ids: territoryIds,
            buildCacheKey: id => $"territories:{territoryKeyField}:{id}",
            buildUrlForMissing: missing =>
            {
                var filters = missing.Select(id => $"name eq '{id.Replace("'", "''")}'");
                return $"{baseUrl}/api/data/v9.2/territories?$select=territoryid,name&$filter=" + string.Join(" or ", filters);
            },
            keyField: "name",                  // côté Dataverse, on cherche par name
            idField: "territoryid",
            token: token,
            logger: logger,
            result: result,
            debug: debug
        );

        // 3) Gammes (référentiel)
        var gammeIds = payload.Mobiliers
            .Where(m => m.IdGamme.HasValue && m.IdGamme.Value != 0)
            .Select(m => m.IdGamme!.Value.ToString())
            .Distinct()
            .ToList();

        await ResolveAndCacheAsync(
            ids: gammeIds,
            buildCacheKey: id => $"dc_gammes:dc_idgamme:{id}",
            buildUrlForMissing: missing =>
            {
                var filters = missing.Select(id => $"dc_idgamme eq '{id.Replace("'", "''")}'");
                return $"{baseUrl}/api/data/v9.2/dc_gammes?$select=dc_gammeid,dc_idgamme&$filter=" + string.Join(" or ", filters);
            },
            keyField: "dc_idgamme",
            idField: "dc_gammeid",
            token: token,
            logger: logger,
            result: result,
            debug: debug
        );

        logger.LogInformation("[Cache] Preload Mobiliers terminé : {Count} clés résolues (Redis/local).", result.Count);
        return result;
    }

    private async Task ResolveAndCacheAsync(
        List<string> ids,
        Func<string, string> buildCacheKey,
        Func<List<string>, string> buildUrlForMissing,
        string keyField,
        string idField,
        string token,
        ILogger logger,
        Dictionary<string, string> result,
        bool debug)
    {
        if (!ids.Any()) return;

        var missing = new List<string>();
        foreach (var id in ids)
        {
            var cacheKey = buildCacheKey(id);
            var cached = await _redis.GetStringAsync(cacheKey);
            if (!string.IsNullOrEmpty(cached))
            {
                result[cacheKey] = cached!;
                _local[cacheKey] = cached!;
                if (debug) logger.LogInformation("[Redis HIT] {Key} = {Val}", cacheKey, cached);
            }
            else
            {
                missing.Add(id);
                if (debug) logger.LogInformation("[Redis MISS] {Key}", cacheKey);
            }
        }

        if (!missing.Any()) return;

        var url = buildUrlForMissing(missing);
        var mapping = await _queryClient.BulkQueryAndMapAsync(url, keyField, idField, token, logger);

        foreach (var kv in mapping)
        {
            var cacheKey = buildCacheKey(kv.Key);
            await _redis.SetStringAsync(cacheKey, kv.Value, TimeSpan.FromDays(90));
            result[cacheKey] = kv.Value;
            _local[cacheKey] = kv.Value;
            if (debug) logger.LogInformation("[Redis SET] {Key} = {Val}", cacheKey, kv.Value);
        }
    }
}
