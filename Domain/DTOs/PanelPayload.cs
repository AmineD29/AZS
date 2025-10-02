// Domain/DTOs/PanelPayload.cs
using System.Text.Json.Serialization;
using WorkOrderFunctions.Domain.Entities;

namespace WorkOrderFunctions.Domain.DTOs
{
    public class PanelPayload
    {
        public string EventType { get; set; } = string.Empty;

        [JsonPropertyName("panels")]
        public List<Panel> Panels { get; set; } = new();
    }
}