using System.Net.Http.Headers;
using System.Text.Json;
using Microsoft.Extensions.Logging;

namespace WorkOrderFunctions.Infrastructure.Dataverse;

public class DataverseQueryClient : IDataverseQueryClient
{
    private static readonly HttpClient _http = new();

    public async Task<JsonDocument> ExecuteQueryAsync(string url, string token, ILogger logger)
    {
        using var req = new HttpRequestMessage(HttpMethod.Get, url);
        req.Headers.Authorization = new AuthenticationHeaderValue("Bearer", token);
        req.Headers.TryAddWithoutValidation("OData-MaxVersion", "4.0");
        req.Headers.TryAddWithoutValidation("OData-Version", "4.0");
        req.Headers.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));
        try
        {
            var resp = await _http.SendAsync(req);
            var content = await resp.Content.ReadAsStringAsync();
            resp.EnsureSuccessStatusCode();
            return JsonDocument.Parse(content);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Erreur ExecuteQueryAsync pour {Url}, {ex.message}", url,ex.Message);
            throw;
        }
    }

    public async Task<Dictionary<string, string>> BulkQueryAndMapAsync(string url, string keyField, string valueField, string token, ILogger logger)
    {
        var result = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        var doc = await ExecuteQueryAsync(url, token, logger);
        if (doc.RootElement.TryGetProperty("value", out var values))
        {
            foreach (var item in values.EnumerateArray())
            {
                string? key = null; string? val = null;
                if (item.TryGetProperty(keyField, out var k)) key = k.ValueKind == JsonValueKind.String ? k.GetString() : k.ToString();
                if (item.TryGetProperty(valueField, out var v)) val = v.ValueKind == JsonValueKind.String ? v.GetString() : v.ToString();
                if (!string.IsNullOrEmpty(key) && !string.IsNullOrEmpty(val)) result[key!] = val!;
            }
        }
        return result;
    }
}
