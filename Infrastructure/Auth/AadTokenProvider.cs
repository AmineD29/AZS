using System.Text.Json;

namespace WorkOrderFunctions.Infrastructure.Auth;

public class AadTokenProvider : ITokenProvider
{
    private static readonly HttpClient _http = new();
    private static string? _accessToken;
    private static DateTime _tokenExpiry;

    public async Task<string> GetAccessTokenAsync()
    {
        if (!string.IsNullOrEmpty(_accessToken) && _tokenExpiry > DateTime.UtcNow.AddMinutes(1))
            return _accessToken!;

        var tenant = Environment.GetEnvironmentVariable("TenantId");
        var clientId = Environment.GetEnvironmentVariable("ClientId");
        var clientSecret = Environment.GetEnvironmentVariable("ClientSecret");
        var resourceUrl = Environment.GetEnvironmentVariable("ResourceUrl");
        var tokenUrl = $"https://login.microsoftonline.com/{tenant}/oauth2/v2.0/token";
        var content = new FormUrlEncodedContent(new Dictionary<string, string>
        {
            ["client_id"] = clientId!,
            ["client_secret"] = clientSecret!,
            ["scope"] = $"{resourceUrl}/.default",
            ["grant_type"] = "client_credentials"
        });
        var resp = await _http.PostAsync(tokenUrl, content);
        resp.EnsureSuccessStatusCode();
        var json = await resp.Content.ReadAsStringAsync();
        using var doc = JsonDocument.Parse(json);
        _accessToken = doc.RootElement.GetProperty("access_token").GetString();
        var expiresIn = doc.RootElement.GetProperty("expires_in").GetInt32();
        _tokenExpiry = DateTime.UtcNow.AddSeconds(expiresIn);
        return _accessToken!;
    }
}
