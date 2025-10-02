using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using WorkOrderFunctions.Application.Orchestrators;
using WorkOrderFunctions.Config;
using WorkOrderFunctions.Functions;
using WorkOrderFunctions.Infrastructure.Auth;
using WorkOrderFunctions.Infrastructure.Caching;
using WorkOrderFunctions.Infrastructure.Dataverse;
using WorkOrderFunctions.Infrastructure.Messaging;
using WorkOrderFunctions.Infrastructure.Resilience;
using WorkOrderFunctions.Policies;
using WorkOrderFunctions.Services;

var host = new HostBuilder()

    .ConfigureFunctionsWebApplication()
    .ConfigureServices(s =>
    {
        // --- Config ---
        s.AddSingleton<AppSettings>();

        // --- Domain/Services métier ---
        s.AddSingleton<IWorkOrderValidator, WorkOrderValidator>();
        s.AddSingleton<IPanelValidator, PanelValidator>();
        s.AddSingleton<IMobilierValidator, MobilierValidator>();
        // --- Auth / AAD ---
        s.AddSingleton<ITokenProvider, AadTokenProvider>();

        // --- Caching / Redis ---
        s.AddSingleton<IRedisCache, RedisCache>();
        s.AddSingleton<IGuidCacheService, GuidCacheService>();

        // --- Dataverse ---
        s.AddSingleton<IDataverseQueryClient, DataverseQueryClient>();
        s.AddSingleton<IDataverseBatchClient, DataverseBatchClient>();

        // --- Résilience ---
        s.AddSingleton<IGlobalCircuitBreaker, RedisCircuitBreaker>();
        s.AddSingleton<IGlobalConcurrencyGate, GlobalBatchSemaphoreGate>();

        // --- Retry Policy (exponential backoff + jitter, partial retry) ---
        s.AddSingleton<IBatchRetryPolicy, ExponentialBackoffRetryPolicy>();

        // --- Messaging / Service Bus ---
        s.AddSingleton<IServiceBusSenderFactory>(_ =>
            new ServiceBusSenderFactory(
                Environment.GetEnvironmentVariable("ServiceBusConnection")
                ?? throw new ArgumentNullException("ServiceBusConnection")));

        s.AddSingleton<DispatcherService>();

       

        // --- Orchestrateur ---
        s.AddSingleton<IWorkOrderOrchestrator, WorkOrderOrchestrator>();
        s.AddSingleton<IPanelOrchestrator, PanelOrchestrator>();
        s.AddSingleton<IMobilierOrchestrator, MobilierOrchestrator>();
        s.AddSingleton<KantarQueueFunction, KantarQueueFunction>();
        s.AddSingleton<FaceQueueFunction, FaceQueueFunction>();
        s.AddSingleton<RessourceQueueToDynamics, RessourceQueueToDynamics>();
    })
    .Build();

host.Run();
