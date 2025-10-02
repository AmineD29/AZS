using Azure.Messaging.ServiceBus;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;
using WorkOrderFunctions.Infrastructure.Auth;

namespace WorkOrderFunctions.Functions
{
    public class KantarQueueFunction
    {
        private readonly ILogger<KantarQueueFunction> _logger;
        private readonly HttpClient httpClient;
        private readonly ITokenProvider _tokenProvider;
        public KantarQueueFunction(ILogger<KantarQueueFunction> logger, HttpClient httpClient, ITokenProvider tokenProvider)
        {
            _logger = logger;
            this.httpClient = httpClient;
            _tokenProvider = tokenProvider;
        }

        [Function("kantarQueueToDynamics")]
        public async Task RunKantar(
            [ServiceBusTrigger("kantar", Connection = "ServiceBusConnection")] string queueMessage,
            ServiceBusReceivedMessage receivedMessage,
            ServiceBusMessageActions messageActions,
            FunctionContext context)
        {
            _logger.LogInformation("=== Début du traitement du message Service Bus ===");
            _logger.LogInformation($"Message reçu depuis la file : kantar");
            _logger.LogInformation($"Contenu du message : {queueMessage}");

            try
            {
                var root = JsonSerializer.Deserialize<JsonElement>(queueMessage);

                string eventType = root.TryGetProperty("eventType", out var eventTypeProp) ? eventTypeProp.GetString() : null;

                if (!root.TryGetProperty("data", out var dataElement) ||
                    !dataElement.TryGetProperty("kantars", out var kantarsArray) ||
                    kantarsArray.ValueKind != JsonValueKind.Array)
                {
                    _logger.LogError("Structure JSON invalide : 'data.kantars' manquant ou invalide.");
                    return;
                }
                var token = await _tokenProvider.GetAccessTokenAsync();
                httpClient.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Bearer", token);

                foreach (var kantar in kantarsArray.EnumerateArray())
                {
                    try
                    {
                        if (!kantar.TryGetProperty("idkantar", out var idKantarProp) || string.IsNullOrWhiteSpace(idKantarProp.GetString()))
                        {
                            _logger.LogError("Champ obligatoire 'idkantar' manquant ou vide. Création annulée.");
                            continue;
                        }
                        string dcIdKantar = idKantarProp.GetString();

                        string dcLibelle = kantar.TryGetProperty("description", out var descProp) ? descProp.GetString() : null;
                        string dcDateDebut = kantar.TryGetProperty("startdate", out var startProp) ? startProp.GetString() : null;
                        string dcDateFin = kantar.TryGetProperty("enddate", out var endProp) ? endProp.GetString() : null;
                        string dcIdPanelCode = kantar.TryGetProperty("idpanel", out var idPanelProp) ? idPanelProp.GetString() : null;

                        if (string.IsNullOrWhiteSpace(dcLibelle) || string.IsNullOrWhiteSpace(dcDateDebut) || string.IsNullOrWhiteSpace(dcDateFin) || string.IsNullOrWhiteSpace(dcIdPanelCode))
                        {
                            _logger.LogError("Champs obligatoires manquants. Ignoré.");
                            continue;
                        }

                        var dynamicsUrl = Environment.GetEnvironmentVariable("Res");

                        var panelGuid = await GetEntityGuidByField("msdyn_customerassets", "dc_idpanel", dcIdPanelCode, "msdyn_customerassetid", token, true);

                        string existingKantarGuid = null;

                        if (eventType == "kantar.deleted")
                        {
                            _logger.LogInformation($"=== Désactivation du kantar {dcIdKantar} ===");

                            var queryUrl = $"{dynamicsUrl}?$filter=dc_idkantar eq '{dcIdKantar}' and _dc_idpanel_value eq {panelGuid} &$select=dc_kantarid";
                            var getResponse = await httpClient.GetAsync(queryUrl);

                            if (getResponse.IsSuccessStatusCode)
                            {
                                var json = await getResponse.Content.ReadAsStringAsync();
                                using var doc = JsonDocument.Parse(json);
                                var values = doc.RootElement.GetProperty("value");

                                if (values.GetArrayLength() > 0)
                                {
                                    existingKantarGuid = values[0].GetProperty("dc_kantarid").GetString();

                                    var deactivateObj = new Dictionary<string, object>
                                    {
                                        { "statecode", 1 },
                                        { "statuscode", 2 }
                                    };

                                    var contentDeactivate = new StringContent(JsonSerializer.Serialize(deactivateObj), Encoding.UTF8, "application/json");
                                    var patchUrl = $"{dynamicsUrl}({existingKantarGuid})";
                                    var patchRequest = new HttpRequestMessage(HttpMethod.Patch, patchUrl) { Content = contentDeactivate };
                                    var deactivateResponse = await httpClient.SendAsync(patchRequest);

                                    if (deactivateResponse.IsSuccessStatusCode)
                                        _logger.LogInformation($"Kantar {dcIdKantar} désactivé avec succès.");
                                    else
                                        _logger.LogError($"Erreur lors de la désactivation du kantar {dcIdKantar}.");
                                }
                                else
                                {
                                    _logger.LogWarning($"Aucun kantar trouvé pour désactivation : {dcIdKantar}");
                                }
                            }
                            else
                            {
                                _logger.LogError($"Erreur lors de la recherche du kantar à désactiver : {await getResponse.Content.ReadAsStringAsync()}");
                            }

                            continue;
                        }

                        if (eventType == "kantar.updated")
                        {
                            var queryUrl = $"{dynamicsUrl}?$filter=dc_idkantar eq '{dcIdKantar}' and _dc_idpanel_value eq {panelGuid} &$select=dc_kantarid";

                            var getResponse = await httpClient.GetAsync(queryUrl);

                            if (getResponse.IsSuccessStatusCode)
                            {
                                var json = await getResponse.Content.ReadAsStringAsync();
                                using var doc = JsonDocument.Parse(json);
                                var values = doc.RootElement.GetProperty("value");

                                if (values.GetArrayLength() > 0)
                                {
                                    existingKantarGuid = values[0].GetProperty("dc_kantarid").GetString();
                                    _logger.LogInformation($"Kantar existant trouvé : {existingKantarGuid}");
                                }
                                else
                                {
                                    _logger.LogWarning($"Aucun kantar existant trouvé pour mise à jour : {dcIdKantar}");
                                    continue;
                                }
                            }
                            else
                            {
                                _logger.LogError($"Erreur lors de la recherche du kantar : {await getResponse.Content.ReadAsStringAsync()}");
                                continue;
                            }
                        }

                        var kantarObj = new Dictionary<string, object>
                        {
                            { "dc_idkantar", dcIdKantar },
                            { "dc_libelle", dcLibelle },
                            { "dc_datededebut", dcDateDebut },
                            { "dc_datedefin", dcDateFin },
                            { "dc_Idpanel@odata.bind", $"/msdyn_customerassets({panelGuid})" }
                        };

                        var content = new StringContent(JsonSerializer.Serialize(kantarObj), Encoding.UTF8, "application/json");
                        HttpResponseMessage? response = null;

                        if (eventType == "kantar.updated" && existingKantarGuid != null)
                        {
                            var patchUrl = $"{dynamicsUrl}({existingKantarGuid})";
                            var patchRequest = new HttpRequestMessage(HttpMethod.Patch, patchUrl) { Content = content };
                            response = await httpClient.SendAsync(patchRequest);
                        }
                        else if (eventType == "kantar.created")
                        {
                            response = await httpClient.PostAsync(dynamicsUrl, content);
                        }

                        if (response != null)
                        {
                            if (response.IsSuccessStatusCode)
                            {
                                _logger.LogInformation($"Kantar {dcIdKantar} {eventType} avec succès dans Dynamics.");
                            }
                            else
                            {
                                var error = await response.Content.ReadAsStringAsync();
                                _logger.LogError($"Erreur Dynamics ({eventType}) : {response.StatusCode} - {error}");
                            }
                        }
                    }
                    catch (Exception ex)
                    {
                        _logger.LogError("Erreur lors du traitement d'un kantar : " + ex.Message);
                        continue;
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError("Exception lors du traitement du message : " + ex.Message);
                _logger.LogError(ex.ToString());
            }

            _logger.LogInformation("=== Fin du traitement du message ===");
        }

        // Placeholders à implémenter dans ton projet
        private Task<string> GetValidAccessTokenAsync() => Task.FromResult(string.Empty);
        private async Task<string> GetEntityGuidByField(string entitySetName, string fieldName, object fieldValue, string guidFieldName, string token, bool isFieldString)
        {
            var dynamicsBaseUrl = Environment.GetEnvironmentVariable("DynamicsBaseUrl");

            // Détection du type pour formater correctement la valeur dans la requête OData

            string formattedValue;

            if (!isFieldString)
            {
                formattedValue = $"'{fieldValue.ToString()}'"; // quotes pour les champs string
            }
            else
            {
                formattedValue = fieldValue.ToString(); // pas de quotes pour les champs int
            }

            var queryUrl = $"{dynamicsBaseUrl}/api/data/v9.2/{entitySetName}?$filter={fieldName} eq {formattedValue}&$select={guidFieldName}";
            using var request = new HttpRequestMessage(HttpMethod.Get, queryUrl);
            request.Headers.Authorization = new AuthenticationHeaderValue("Bearer", token);
            request.Headers.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));

            var response = await httpClient.SendAsync(request);

            var content = await response.Content.ReadAsStringAsync();

            if (response.IsSuccessStatusCode)
            {
                var json = JsonDocument.Parse(content);
                var entity = json.RootElement.GetProperty("value").EnumerateArray().FirstOrDefault();

                if (entity.ValueKind != JsonValueKind.Undefined && entity.TryGetProperty(guidFieldName, out var guid))
                {
                    return guid.GetString()?.Replace(" ", "").ToLower();
                }

                _logger.LogWarning($"Aucun enregistrement trouvé dans {entitySetName} avec {fieldName} = {fieldValue}");
            }
            else
            {
                _logger.LogError($"Erreur lors de la requête GET vers Dynamics : {response.StatusCode} - {content}");
            }

            return null;
        }
    }
}