using WorkOrderFunctions.Domain.Entities;

namespace WorkOrderFunctions.Services;

public interface IWorkOrderValidator
{
    bool IsValidWorkOrder(WorkOrder workorder);
    bool IsValidSubWorkOrder(SubWorkOrder child);
}
