using Azure.Messaging.ServiceBus;

namespace WorkOrderFunctions.Infrastructure.Messaging;

public class ServiceBusSenderFactory : IServiceBusSenderFactory, IAsyncDisposable
{
    private readonly ServiceBusClient _client;
    public ServiceBusSenderFactory(string connectionString) => _client = new(connectionString);
    public ServiceBusSender CreateSender(string name) => _client.CreateSender(name);
    public ValueTask DisposeAsync() => _client.DisposeAsync();
}
