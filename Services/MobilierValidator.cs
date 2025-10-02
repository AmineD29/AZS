
using WorkOrderFunctions.Domain.Entities;
using WorkOrderFunctions.Services;


    public class MobilierValidator : IMobilierValidator
{
        public bool IsValid(WorkOrderFunctions.Domain.Entities.Mobilier m)
            => m is not null
               && m.IdStructure > 0
               && !string.IsNullOrWhiteSpace(m.Name);
    }


