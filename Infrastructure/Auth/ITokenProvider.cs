namespace WorkOrderFunctions.Infrastructure.Auth;

public interface ITokenProvider
{
    Task<string> GetAccessTokenAsync();
}
