using Azure.Messaging.ServiceBus;

namespace WorkOrderFunctions.Infrastructure.Messaging;

public interface IServiceBusSenderFactory
{
    ServiceBusSender CreateSender(string queueOrTopicName);
}
