using System;
using System.Linq;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Text;
using System.Text.Json;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using System.Collections.Generic;
using Microsoft.Extensions.Logging;
using WorkOrderFunctions.Config;

namespace WorkOrderFunctions.Infrastructure.Dataverse
{
    public class DataverseBatchClient : IDataverseBatchClient
    {
        private static readonly HttpClient _http = new();
        private readonly AppSettings _settings;

        public DataverseBatchClient(AppSettings settings) => _settings = settings;

        public async Task<BatchExecutionResult> ExecuteBatchAsync(
            List<(Dictionary<string, object> Payload, string EntitySetName, int ParentContentId)> operations,
            string token,
            ILogger logger,
            bool createOnly)
        {
            var resultTemplate = new BatchExecutionResult(
                IsSuccess: false,
                IsThrottled: false,
                SuccessCount: 0,
                FailedIndices: new List<int>(),
                ErrorMessage: null,
                RetryAfter: null);

            if (operations == null || operations.Count == 0)
                return resultTemplate with { IsSuccess = true };

            string batchId = Guid.NewGuid().ToString();
            string changesetId = Guid.NewGuid().ToString();

            var sb = new StringBuilder();
            sb.AppendLine($"--batch_{batchId}");
            sb.AppendLine($"Content-Type: multipart/mixed; boundary=changeset_{changesetId}");
            sb.AppendLine();

            int contentId = 1;
            bool debug = string.Equals(Environment.GetEnvironmentVariable("BatchDebug"), "true", StringComparison.OrdinalIgnoreCase);

            foreach (var (payload, entitySetName, parentContentId) in operations)
            {
                sb.AppendLine($"--changeset_{changesetId}");
                sb.AppendLine("Content-Type: application/http");
                sb.AppendLine("Content-Transfer-Encoding: binary");
                sb.AppendLine($"Content-ID: {contentId}");
                sb.AppendLine();

                var httpMethod = "PATCH";

                var entityAltKeyAttr = Environment.GetEnvironmentVariable($"AltKey_{entitySetName}");
                if (string.IsNullOrWhiteSpace(entityAltKeyAttr) || !payload.TryGetValue(entityAltKeyAttr, out var akObj) || akObj is null)
                {
                    logger.LogError("Clé alternative '{AltKey}' manquante pour l'entité '{EntitySetName}' dans le payload: {Payload}",
                        entityAltKeyAttr, entitySetName, JsonSerializer.Serialize(payload));
                    throw new InvalidOperationException($"Clé alternative '{entityAltKeyAttr}' manquante pour l'entité '{entitySetName}'.");
                }

                string akStr = akObj?.ToString() ?? string.Empty;
                string targetUrl = akObj is int or long
                    ? $"{_settings.ResourceUrl}/api/data/v9.2/{entitySetName}({entityAltKeyAttr}={akStr})"
                    : $"{_settings.ResourceUrl}/api/data/v9.2/{entitySetName}({entityAltKeyAttr}='{Uri.EscapeDataString(akStr.Replace("'", "''"))}')";


                sb.AppendLine($"{httpMethod} {targetUrl} HTTP/1.1");
                sb.AppendLine("Content-Type: application/json; charset=utf-8");
                //sb.AppendLine("If-None-Match: *");
                sb.AppendLine();

                var body = JsonSerializer.Serialize(payload);
                sb.AppendLine(body);
                sb.AppendLine();

                if (debug)
                {
                    var truncated = body.Length > 2000 ? body.Substring(0, 2000) + " ..." : body;
                    logger.LogInformation("[Batch] op#{ContentId}: {Method} {Url} (createOnly={CreateOnly}) Body={Body}",
                        contentId, httpMethod, targetUrl, createOnly, truncated);
                }

                contentId++;
            }

            sb.AppendLine($"--changeset_{changesetId}--");
            sb.AppendLine($"--batch_{batchId}--");

            var request = new HttpRequestMessage(HttpMethod.Post, $"{_settings.ResourceUrl}/api/data/v9.2/$batch");
            request.Headers.Authorization = new AuthenticationHeaderValue("Bearer", token);
            request.Content = new StringContent(sb.ToString(), Encoding.UTF8);
            request.Content.Headers.ContentType = new MediaTypeHeaderValue("multipart/mixed");
            request.Content.Headers.ContentType.Parameters.Add(new NameValueHeaderValue("boundary", $"batch_{batchId}"));

            HttpResponseMessage httpResp;
            string content;
            TimeSpan? retryAfterDuration = null;

            try
            {
                httpResp = await _http.SendAsync(request);
                content = await httpResp.Content.ReadAsStringAsync();
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "Erreur d'appel HTTP au $batch Dataverse.");
                return resultTemplate with { IsSuccess = false, ErrorMessage = ex.Message };
            }

            if ((int)httpResp.StatusCode == 429 || (int)httpResp.StatusCode == 503 || (int)httpResp.StatusCode == 502 || (int)httpResp.StatusCode == 504)
            {

                if (httpResp.Headers.TryGetValues("Retry-After", out var values))
                {
                    var retryAfter = values.FirstOrDefault();
                    if (int.TryParse(retryAfter, out int seconds))
                        retryAfterDuration = TimeSpan.FromSeconds(seconds);
                    else if (DateTime.TryParse(retryAfter, out DateTime retryDate))
                        retryAfterDuration = retryDate - DateTime.UtcNow;
                }
            }

            if ((int)httpResp.StatusCode == 429
                || (int)httpResp.StatusCode == 503
                || (int)httpResp.StatusCode == 502
                || (int)httpResp.StatusCode == 504
                || content.Contains("0x80072321", StringComparison.OrdinalIgnoreCase))
            {
                return resultTemplate with
                {
                    IsSuccess = false,
                    IsThrottled = true,
                    ErrorMessage = $"HTTP {(int)httpResp.StatusCode} throttled or dataverse limit reached.",
                    RetryAfter = retryAfterDuration
                };
            }

            if (!httpResp.IsSuccessStatusCode)
            {
                logger.LogError("Batch HTTP {Status}: {Body}", (int)httpResp.StatusCode, content);
                return resultTemplate with
                {
                    IsSuccess = false,
                    IsThrottled = false,
                    ErrorMessage = $"HTTP {(int)httpResp.StatusCode}: {content}"
                };
            }

            var statusMatches = Regex.Matches(content, @"HTTP/1\.[01]\s+(\d{3})");
            int success = 0;
            var failedIndices = new List<int>();

            if (statusMatches.Count < operations.Count)
            {
                logger.LogWarning("Batch parsing: {Found} sous-statuts pour {Ops} opérations. Réponse tronquée ?",
                    statusMatches.Count, operations.Count);
            }

            for (int i = 0; i < Math.Min(statusMatches.Count, operations.Count); i++)
            {
                int status = int.Parse(statusMatches[i].Groups[1].Value);
                if (status >= 200 && status < 300)
                {
                    success++;
                }
                else if (status == 412)
                {
                    success++;
                }
                else
                {
                    failedIndices.Add(i);
                }
            }

            return resultTemplate with
            {
                IsSuccess = failedIndices.Count == 0,
                IsThrottled = false,
                SuccessCount = success,
                FailedIndices = failedIndices,
                ErrorMessage = failedIndices.Count == 0 ? null : $"Some operations failed: {string.Join(",", failedIndices)}"
            };
        }
    }
}