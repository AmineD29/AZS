using System.Text.Json;
using Microsoft.Extensions.Logging;

namespace WorkOrderFunctions.Infrastructure.Dataverse;

public interface IDataverseQueryClient
{
    Task<JsonDocument> ExecuteQueryAsync(string url, string token, ILogger logger);
    Task<Dictionary<string, string>> BulkQueryAndMapAsync(string url, string keyField, string valueField, string token, ILogger logger);
}
