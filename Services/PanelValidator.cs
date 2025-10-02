
using WorkOrderFunctions.Domain.Entities;

public class PanelValidator : IPanelValidator
{
    public bool IsValid(Panel panel)
    {
        return !string.IsNullOrWhiteSpace(panel.RefPanel) && panel.IdPanel > 0;
    }
}
