using System.Text.Json.Serialization;

namespace WorkOrderFunctions.Domain.Entities;

public class WorkOrder
{
    [JsonPropertyName("isinternal")] public bool IsInternal { get; set; }
    [JsonPropertyName("idstructure")] public long IdStructure { get; set; }
    [JsonPropertyName("ottype")] public string OtType { get; set; } = string.Empty;
    [JsonPropertyName("panel")] public List<string> Panel { get; set; } = new();
    [JsonPropertyName("resourceid")] public long ResourceId { get; set; }
    [JsonPropertyName("subworkorder")] public List<SubWorkOrder> SubWorkOrders { get; set; } = new();
    [JsonPropertyName("WorkOrderBroadcastId")] public string WorkOrderBroadcastId { get; set; } = string.Empty;
}
