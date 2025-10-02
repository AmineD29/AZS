using WorkOrderFunctions.Domain.Entities;

namespace WorkOrderFunctions.Services;

public class WorkOrderValidator : IWorkOrderValidator
{
    public bool IsValidWorkOrder(WorkOrder w)
        => w.IdStructure > 0
           && !string.IsNullOrWhiteSpace(w.WorkOrderBroadcastId)
           && !string.IsNullOrWhiteSpace(w.OtType)
           && w.ResourceId > 0
           && w.Panel != null && w.Panel.Count > 0;

    public bool IsValidSubWorkOrder(SubWorkOrder c)
        => !string.IsNullOrWhiteSpace(c.SubWorkOrderId)
           && !string.IsNullOrWhiteSpace(c.Type)
           && c.IdFace > 0
           && !string.IsNullOrWhiteSpace(c.IdPanel);
}
