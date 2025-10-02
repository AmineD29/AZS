namespace WorkOrderFunctions.Domain.Events;

public class EventGridEnvelope
{
    public string Id { get; set; } = string.Empty;
    public string Subject { get; set; } = string.Empty;
    public string EventType { get; set; } = string.Empty;
    public DateTime EventTime { get; set; }
    public EventGridData Data { get; set; } = new();
}
