// src/WorkOrderFunctions.Application/Orchestrators/IMobilierOrchestrator.cs
 namespace WorkOrderFunctions.Application.Orchestrators;
 using Microsoft.Extensions.Logging;

 public interface IMobilierOrchestrator
 {
     // Même pattern que IPanelOrchestrator : une surcharge avec createOnly.
     Task ProcessAsync(string queueMessage, ILogger logger);
     Task ProcessAsync(string queueMessage, ILogger logger, bool createOnly);
 }