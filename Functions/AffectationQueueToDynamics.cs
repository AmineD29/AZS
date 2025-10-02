using Azure.Messaging.ServiceBus;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;
using System.Net.Http.Headers;
using System.Text;
using System.Text.Json;
using WorkOrderFunctions.Infrastructure.Auth;

namespace WorkOrderCreate.Functions
{
    public class AffectationQueueFunction
    {
        private readonly ILogger<AffectationQueueFunction> _logger;
        private readonly HttpClient httpClient;
        private readonly ITokenProvider _tokenProvider;
        public AffectationQueueFunction(ILogger<AffectationQueueFunction> logger, HttpClient httpClient, ITokenProvider tokenProvider)
        {
            _logger = logger;
            this.httpClient = httpClient;
            _tokenProvider = tokenProvider;
        }
        [Function("AffectationQueueToDynamics")]
        public async Task RunAffectation(
            [ServiceBusTrigger("affectation", Connection = "ServiceBusConnection")] string queueMessage, ServiceBusReceivedMessage receivedMessage, ServiceBusMessageActions messageActions, FunctionContext context)
        {
            _logger.LogInformation("=== Début du traitement du message Service Bus ===");
            _logger.LogInformation($"Message reçu depuis la file : affectation");
            _logger.LogInformation($"Contenu du message : {queueMessage}");

            try
            {
                var root = JsonSerializer.Deserialize<JsonElement>(queueMessage);
                string eventType = root.TryGetProperty("eventType", out var eventTypeProp)
                                       ? eventTypeProp.GetString()
                                       : null;

                if (!root.TryGetProperty("data", out var dataElement) ||
                    !dataElement.TryGetProperty("affectations", out var affectationsArray) || affectationsArray.ValueKind != JsonValueKind.Array)
                {
                    _logger.LogError("Structure JSON invalide : 'data.affectations' manquant ou invalide.");
                    return;
                }

                var dynamicsUrlAffectation = Environment.GetEnvironmentVariable("DynamicsApiUrlAffectation");
                var token = await _tokenProvider.GetAccessTokenAsync();
                httpClient.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Bearer", token);
                httpClient.DefaultRequestHeaders.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));


                foreach (var affectation in affectationsArray.EnumerateArray())
                {
                    try
                    {
                        if (!affectation.TryGetProperty("idressource", out var idressourceProp) ||
                            !idressourceProp.TryGetInt32(out var dc_idressource))
                        {
                            _logger.LogError("Champ obligatoire 'idressource' manquant ou invalide.");
                            continue;
                        }

                        if (!affectation.TryGetProperty("startdate", out var startProp) ||
                            string.IsNullOrWhiteSpace(startProp.GetString()))
                        {
                            _logger.LogError("Champ obligatoire 'startdate' manquant ou vide.");
                            continue;
                        }
                        string dc_DateDebut = startProp.GetString();

                        string dc_DateFin = affectation.TryGetProperty("enddate", out var endProp) ? endProp.GetString() : null;

                        if (!affectation.TryGetProperty("idaffectation", out var idaffectationProp) || !idaffectationProp.TryGetInt32(out var idaffectationVal))
                        {
                            _logger.LogError("Champ obligatoire 'idaffectation' manquant ou invalide.");
                            continue;
                        }

                        if (!affectation.TryGetProperty("idpanel", out var idpanelProp) || !idpanelProp.TryGetInt32(out var idpanelVal))
                        {
                            _logger.LogError("Champ obligatoire 'idpanel' manquant ou invalide.");
                            continue;
                        }

                        string activity = affectation.TryGetProperty("activity", out var activityProp) ? activityProp.GetString() : null;






                        // Résolution du GUID pour idpanel et type d'activité
                        var panelGuid = await GetEntityGuidByField("msdyn_customerassets", "dc_idpanel", idpanelProp, "msdyn_customerassetid", token, true);
                        var worktypeGuid = await GetEntityGuidByField("msdyn_workordertypes", "msdyn_name", activity, "msdyn_workordertypeid", token, false);
                        var ressourceGuid = await GetEntityGuidByField("bookableresources", "dc_idressource", dc_idressource, "bookableresourceid", token, true);


                        if (string.IsNullOrEmpty(panelGuid) || string.IsNullOrEmpty(worktypeGuid) || string.IsNullOrEmpty(ressourceGuid))
                        {
                            _logger.LogError("Un des GUIDs obligatoires du parent est introuvable (panel/type/ressource). affectation ignoré.");
                            continue;
                        }

                        var queryUrl = $"{dynamicsUrlAffectation}?$filter=_dc_idressource_value eq {ressourceGuid} and _dc_idpanel_value eq {panelGuid} and dc_idaffecation eq '{idaffectationVal}' &$select=dc_affectationid";
                        var getResponse = await httpClient.GetAsync(queryUrl);

                        string existingAffectationGuid = null;
                        if (getResponse.IsSuccessStatusCode)
                        {
                            var json = await getResponse.Content.ReadAsStringAsync();
                            using var doc = JsonDocument.Parse(json);
                            var values = doc.RootElement.GetProperty("value");
                            if (values.GetArrayLength() > 0)
                                //existingAffectationGuid = values[0].GetProperty("idaffectationVal").GetString();
                                existingAffectationGuid = values[0].GetProperty("dc_affectationid").GetString();
                        }
                        else
                        {
                            _logger.LogError($"Erreur lors de la vérification de l'affectation : {await getResponse.Content.ReadAsStringAsync()}");
                            continue;
                        }
                        // Création du record
                        var payload = new Dictionary<string, object>
                            {
                                { "dc_idaffecation", idaffectationVal.ToString() },
                                { "dc_datedebut", dc_DateDebut },
                                { "dc_datefin", string.IsNullOrEmpty(dc_DateFin) ? null : dc_DateFin },
                                { "dc_Idpanel@odata.bind", $"/msdyn_customerassets({panelGuid})" },
                                { "dc_Idressource@odata.bind", $"/bookableresources({ressourceGuid})" },
                                { "dc_Typedactivite@odata.bind", $"/msdyn_workordertypes({worktypeGuid})" }
                            };
                        var content = new StringContent(JsonSerializer.Serialize(payload), Encoding.UTF8, "application/json");
                        HttpResponseMessage? response = null;

                        if (eventType == "affectation.created")
                        {
                            if (!string.IsNullOrEmpty(existingAffectationGuid))
                            {
                                _logger.LogWarning($"L'affectation {idaffectationVal} existe déjà pour cette ressource et panel. Création ignorée.");
                                continue;
                            }
                            response = await httpClient.PostAsync(dynamicsUrlAffectation, content);
                        }

                        else if (eventType == "affectation.updated")
                        {
                            if (string.IsNullOrEmpty(existingAffectationGuid))
                            {
                                _logger.LogWarning($"Affectation {idaffectationVal} introuvable pour update. Ignorée.");
                                continue;
                            }
                            var patchUrl = $"{dynamicsUrlAffectation}({existingAffectationGuid})";
                            var patchRequest = new HttpRequestMessage(HttpMethod.Patch, patchUrl) { Content = content };
                            response = await httpClient.SendAsync(patchRequest);
                        }

                        if (response != null)
                        {
                            if (response.IsSuccessStatusCode)
                                _logger.LogInformation($"Affectation {idaffectationVal} {eventType} avec succès.");
                            else
                                _logger.LogError($"Erreur Dynamics ({eventType}) affectation {idaffectationVal}: {await response.Content.ReadAsStringAsync()}");
                        }
                        // Si création/update réussie -> mettre à jour le panel
                        if (response != null && response.IsSuccessStatusCode)
                        {
                            var panelUpdatePayload = new Dictionary<string, object>
                    {
                        { "dc_PreferredResource@odata.bind", $"/bookableresources({ressourceGuid})" }
                    };
                            var panelUpdateUrl = $"https://fieldservice-dev.crm12.dynamics.com/api/data/v9.2/msdyn_customerassets({panelGuid})";
                            var panelResponse = await httpClient.SendAsync(new HttpRequestMessage(HttpMethod.Patch, panelUpdateUrl)
                            {
                                Content = new StringContent(JsonSerializer.Serialize(panelUpdatePayload), Encoding.UTF8, "application/json")
                            });

                            if (!panelResponse.IsSuccessStatusCode)

                                _logger.LogError($"Erreur mise à jour panel {idpanelVal} : {await panelResponse.Content.ReadAsStringAsync()}");
                        }
                        else if (response != null)
                        {
                            _logger.LogError($"Erreur Dynamics ({eventType}) affectation {idaffectationVal}: {await response.Content.ReadAsStringAsync()}");
                        }
                    }
                    catch (Exception ex)
                    {
                        _logger.LogError("Erreur lors du traitement d'une affectation : " + ex.Message);
                        _logger.LogError(ex.ToString());
                        // Acheminement vers le DLQ
                        continue;  // Pour continuer avec les autres affectations
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError("Exception lors du traitement du message : " + ex.Message);
                _logger.LogError(ex.ToString());
                // Acheminement vers le DLQ

            }

            _logger.LogInformation("=== Fin du traitement du message ===");
        }

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
