namespace WorkOrderFunctions.Config;

public class AppSettings
{
    public string ResourceUrl { get; }
    public int BatchMaxOperations { get; }
    public int MaxConcurrentBatches { get; }
    public int BatchMaxAttempts { get; }
    public int BatchInitialBackoffSeconds { get; }
    public int BatchMaxBackoffSeconds { get; }

    public AppSettings()
    {
        ResourceUrl = Environment.GetEnvironmentVariable("ResourceUrl") ?? throw new ArgumentNullException("ResourceUrl");
        BatchMaxOperations = int.TryParse(Environment.GetEnvironmentVariable("BatchSize"), out var b) ? Math.Min(Math.Max(b, 10), 1000) : 1000;
        MaxConcurrentBatches = int.TryParse(Environment.GetEnvironmentVariable("MaxConcurrentBatches"), out var m) ? Math.Max(1, m) : 4;
        BatchMaxAttempts = int.TryParse(Environment.GetEnvironmentVariable("BatchMaxAttempts"), out var c) ? Math.Max(1, c) : 5;
        BatchInitialBackoffSeconds = int.TryParse(Environment.GetEnvironmentVariable("BatchInitialBackoffSeconds"), out var d) ? Math.Max(1, d) : 1;
        BatchMaxBackoffSeconds = int.TryParse(Environment.GetEnvironmentVariable("BatchMaxBackoffSeconds"), out var t) ? Math.Max(1, t) : 30;
    }
}
