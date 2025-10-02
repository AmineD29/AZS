// src/WorkOrderFunctions.Infrastructure/Caching/IGuidCacheService.cs
using Microsoft.Extensions.Logging;
using WorkOrderFunctions.Domain.DTOs;

namespace WorkOrderFunctions.Infrastructure.Caching
{
    public interface IGuidCacheService
    {
        Task<IDictionary<string, string>> PreloadAsync(WorkOrderPayload payload, string token, ILogger logger);
        string? TryGet(string key);
        Task<IDictionary<string, string>> PreloadPanelsAsync(PanelPayload payload, string token, ILogger logger);

        Task<IDictionary<string, string>> PreloadMobiliersAsync(MobilierPayload payload, string token, ILogger logger);
    }
}