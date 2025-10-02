using System.Text.Json.Serialization;

namespace WorkOrderFunctions.Domain.Entities;

public class SubWorkOrder
{
    [JsonPropertyName("subworkorderid")] public string SubWorkOrderId { get; set; } = string.Empty;
    [JsonPropertyName("isaudited")] public bool IsAudited { get; set; }
    [JsonPropertyName("theme")] public string? Theme { get; set; }
    [JsonPropertyName("advertiserlabel")] public string? AdvertiserLabel { get; set; }
    [JsonPropertyName("salesfolder")] public string? SalesFolder { get; set; }
    [JsonPropertyName("issuspended")] public bool IsSuspended { get; set; }
    [JsonPropertyName("iskeepedinplace")] public bool IsKeepedInPlace { get; set; }
    [JsonPropertyName("type")] public string Type { get; set; } = string.Empty;
    [JsonPropertyName("idpanel")] public string IdPanel { get; set; } = string.Empty;
    [JsonPropertyName("idface")] public long IdFace { get; set; }
    [JsonPropertyName("bandeau")] public string? Bandeau { get; set; }
    [JsonPropertyName("flap")] public string? Flap { get; set; }
    [JsonPropertyName("managername")] public string? ManagerName { get; set; }
    [JsonPropertyName("comment")] public string? Comment { get; set; }
    [JsonPropertyName("reason")] public string? Reason { get; set; }
    [JsonPropertyName("ismandatoryphoto")] public bool IsMandatoryPhoto { get; set; }
    [JsonPropertyName("isleftinplace")] public bool IsLeftInPlace { get; set; }
    [JsonPropertyName("iscanceled")] public bool IsCanceled { get; set; }
    [JsonPropertyName("startdate")] public string? StartDate { get; set; }
    [JsonPropertyName("enddate")] public string? EndDate { get; set; }
    [JsonPropertyName("starthour")] public string? StartHour { get; set; }
    [JsonPropertyName("endhour")] public string? EndHour { get; set; }
}
