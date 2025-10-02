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
    public class FaceQueueFunction
    {
        private readonly ILogger<FaceQueueFunction> _logger;
        private readonly HttpClient httpClient; 
        private readonly ITokenProvider _tokenProvider;
        public FaceQueueFunction(ILogger<FaceQueueFunction> logger, HttpClient httpClient, ITokenProvider tokenProvider)
        {
            _logger = logger;
            this.httpClient = httpClient;
            _tokenProvider = tokenProvider;
        }

        [Function("FaceQueueToDynamics")]
        public async Task RunFace(
            [ServiceBusTrigger("face", Connection = "ServiceBusConnection")] string queueMessage,
            ServiceBusReceivedMessage receivedMessage,
            ServiceBusMessageActions messageActions,
            FunctionContext context)
        {
            _logger.LogInformation("=== Début du traitement du message Service Bus ===");
            _logger.LogInformation($"Message reçu depuis la file : face");
            _logger.LogInformation($"Contenu du message : {queueMessage}");

            try
            {
                var root = JsonSerializer.Deserialize<JsonElement>(queueMessage);

                string eventType = root.TryGetProperty("eventType", out var eventTypeProp) ? eventTypeProp.GetString() : null;

                if (!root.TryGetProperty("data", out var dataElement) ||
                    !dataElement.TryGetProperty("faces", out var facesArray) ||
                    facesArray.ValueKind != JsonValueKind.Array)
                {
                    _logger.LogError("Structure JSON invalide : 'data.faces' manquant ou invalide.");
                    return;
                }

                var token = await _tokenProvider.GetAccessTokenAsync();
             
                httpClient.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Bearer", token);

                foreach (var face in facesArray.EnumerateArray())
                {
                    try
                    {
                        if (!face.TryGetProperty("refface", out var reffaceProp) || string.IsNullOrWhiteSpace(reffaceProp.GetString()))
                        {
                            _logger.LogError("Champ obligatoire 'refface' manquant. Annulation.");
                            continue;
                        }
                        var refface = reffaceProp.GetString();

                        if (!face.TryGetProperty("idface", out var idfaceProp) || !idfaceProp.TryGetInt32(out var idfaceVal))
                        {
                            _logger.LogError("Champ obligatoire 'idface' manquant ou invalide. Annulation.");
                            continue;
                        }
                        int idface = idfaceVal;

                        if (!face.TryGetProperty("idpanel", out var idpanelProp) || !idpanelProp.TryGetInt32(out var idpanelVal))
                        {
                            _logger.LogError("Champ obligatoire 'idpanel' manquant ou invalide. Annulation.");
                            continue;
                        }
                        int idpanel = idpanelVal;

                        if (!face.TryGetProperty("grattage", out var grattageProp) ||
                            (grattageProp.ValueKind != JsonValueKind.True && grattageProp.ValueKind != JsonValueKind.False))
                        {
                            _logger.LogError("Champ obligatoire 'grattage' manquant ou invalide. Annulation.");
                            continue;
                        }
                        bool Grattage = grattageProp.GetBoolean();

                        if (!face.TryGetProperty("status", out var statusProp) || statusProp.ValueKind != JsonValueKind.String)
                        {
                            _logger.LogError("Champ obligatoire 'status' manquant ou invalide. Annulation.");
                            continue;
                        }
                        var statusStr = statusProp.GetString();
                        if (statusStr != "0" && statusStr != "1")
                        {
                            _logger.LogError("Champ 'status' doit être 0 ou 1. Annulation.");
                            continue;
                        }
                        int stateCode = int.Parse(statusStr);

                        var dynamicsUrl = Environment.GetEnvironmentVariable("DynamicsApiUrlPanel");
                        string existingFaceGuid = null;

                        if (eventType == "face.updated")
                        {
                            var queryUrl = $"{dynamicsUrl}?$filter=msdyn_name eq '{refface}'&$select=msdyn_customerassetid";
                            var getResponse = await httpClient.GetAsync(queryUrl);
                            if (getResponse.IsSuccessStatusCode)
                            {
                                var json = await getResponse.Content.ReadAsStringAsync();
                                using var doc = JsonDocument.Parse(json);
                                var values = doc.RootElement.GetProperty("value");
                                if (values.GetArrayLength() > 0)
                                {
                                    existingFaceGuid = values[0].GetProperty("msdyn_customerassetid").GetString();
                                    _logger.LogInformation($"face existant trouvé : {existingFaceGuid}");
                                }
                                else
                                {
                                    _logger.LogWarning($"Aucune face trouvé avec refface = {refface}. Update annulée.");
                                    continue;
                                }
                            }
                            else
                            {
                                _logger.LogError($"Erreur lors de la recherche du kantar : {await getResponse.Content.ReadAsStringAsync()}");
                                continue;
                            }
                        }

                        var panelGuid = await GetEntityGuidByField("msdyn_customerassets", "dc_idpanel", idpanel, "msdyn_customerassetid", token, true);
                        if (string.IsNullOrEmpty(panelGuid))
                        {
                            _logger.LogError($"Le panel avec idpanel={idpanel} n'existe pas dans Dynamics.");
                            continue;
                        }

                        var actif = new Dictionary<string, object>
                        {
                            { "msdyn_name", refface },
                            { "dc_grattage", Grattage }
                        };
                        if (!string.IsNullOrEmpty(panelGuid))
                        {
                            actif["msdyn_parentasset_msdyn_customerasset@odata.bind"] = $"/msdyn_customerassets({panelGuid})";
                        }
                        actif["dc_idface"] = idface;
                        actif["statecode"] = stateCode;

                        var content = new StringContent(JsonSerializer.Serialize(actif), Encoding.UTF8, "application/json");
                        _logger.LogInformation($"Envoi du POST vers Dynamics : {dynamicsUrl}");
                        _logger.LogInformation($"Payload : {JsonSerializer.Serialize(actif)}");

                        HttpResponseMessage response;
                        if (eventType == "face.updated" && existingFaceGuid != null)
                        {
                            var patchUrl = $"{dynamicsUrl}({existingFaceGuid})";
                            _logger.LogInformation($"Envoi du PATCH vers Dynamics : {patchUrl}");
                            var patchRequest = new HttpRequestMessage(new HttpMethod("PATCH"), patchUrl)
                            {
                                Content = content
                            };
                            response = await httpClient.SendAsync(patchRequest);
                        }
                        else
                        {
                            _logger.LogInformation($"Envoi du POST vers Dynamics : {dynamicsUrl}");
                            response = await httpClient.PostAsync(dynamicsUrl, content);

                            if (response.IsSuccessStatusCode && stateCode == 1 && eventType != "face.updated")
                            {
                                if (response.Headers.TryGetValues("OData-EntityId", out var entityValues))
                                {
                                    var createdFaceUrl = entityValues.FirstOrDefault();
                                    if (!string.IsNullOrEmpty(createdFaceUrl))
                                    {
                                        var patchContent = new StringContent(
                                            JsonSerializer.Serialize(new { statecode = 1 }),
                                            Encoding.UTF8,
                                            "application/json"
                                        );
                                        var patchRequest = new HttpRequestMessage(new HttpMethod("PATCH"), createdFaceUrl)
                                        {
                                            Content = patchContent
                                        };
                                        var disableResponse = await httpClient.SendAsync(patchRequest);
                                        if (!disableResponse.IsSuccessStatusCode)
                                        {
                                            _logger.LogError($"Erreur lors de la désactivation de la face créée : {disableResponse.StatusCode} - {await disableResponse.Content.ReadAsStringAsync()}");
                                        }
                                    }
                                }
                            }
                        }

                        if (response.IsSuccessStatusCode)
                        {
                            if (eventType != "face.updated")
                            {
                                _logger.LogInformation("face créé avec succès dans Dynamics.");
                            }
                            else
                            {
                                _logger.LogInformation("face mis à jour avec succès dans Dynamics.");
                            }
                        }
                        else
                        {
                            var error = await response.Content.ReadAsStringAsync();
                            _logger.LogError($"Erreur Dynamics : {response.StatusCode} - {error}");
                        }
                    }
                    catch (Exception ex)
                    {
                        _logger.LogError("Erreur lors du traitement d'une face : " + ex.Message);
                        _logger.LogError(ex.ToString());
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