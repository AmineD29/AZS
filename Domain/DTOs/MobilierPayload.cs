// src/WorkOrderFunctions.Domain/DTOs/MobilierPayload.cs
 using System.Text.Json.Serialization;
 using WorkOrderFunctions.Domain.Entities;

 namespace WorkOrderFunctions.Domain.DTOs
 {
     public sealed class MobilierPayload
     {
         [JsonPropertyName("eventType")]
         public string EventType { get; set; } = string.Empty;

         [JsonPropertyName("mobiliers")]
         public List<Mobilier> Mobiliers { get; set; } = new();
     }
 }