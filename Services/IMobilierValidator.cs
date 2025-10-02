using WorkOrderFunctions.Domain.Entities;

namespace WorkOrderFunctions.Services;

public interface IMobilierValidator
 {
     bool IsValid(Mobilier item);
 }