using System.Text.Json.Serialization;
using WorkOrderFunctions.Domain.Entities;

namespace WorkOrderFunctions.Domain.DTOs;

public class WorkOrderPayload
{
    public string EventType { get; set; } = string.Empty;

    [JsonPropertyName("workorders")]
    public List<WorkOrder> WorkOrders { get; set; } = new();
}
