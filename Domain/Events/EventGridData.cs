using WorkOrderFunctions.Domain.Entities;

namespace WorkOrderFunctions.Domain.Events;

public class EventGridData
{
    public List<WorkOrder> WorkOrders { get; set; } = new();
}
