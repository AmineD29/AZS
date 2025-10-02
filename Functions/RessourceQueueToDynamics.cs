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
    public class RessourceQueueToDynamics
    {
        private readonly ILogger<RessourceQueueToDynamics> _logger;
        private readonly HttpClient httpClient;
        private readonly ITokenProvider _tokenProvider;
        public RessourceQueueToDynamics(ILogger<RessourceQueueToDynamics> logger, HttpClient httpClient, ITokenProvider tokenProvider)
        {
            _logger = logger;
            this.httpClient = httpClient;
            _tokenProvider = tokenProvider;
        }


        [Function("RessourceQueueToDynamics")]
        public async Task RunRessource(
    [
      ServiceBusTrigger("ressource", Connection = "ServiceBusConnection")
    ] string queueMessage,
    ServiceBusReceivedMessage receivedMessage,
    ServiceBusMessageActions messageActions, FunctionContext context)
        {
            _logger.LogInformation("=== Début du traitement du message Service Bus ===");
            _logger.LogInformation($"Message reçu depuis la file : ressource");
            _logger.LogInformation($"Contenu du message : {queueMessage}");

            try
            {
                var root = JsonSerializer.Deserialize<JsonElement>(queueMessage);
                string eventType = root.TryGetProperty("eventType", out var eventTypeProp)
                                       ? eventTypeProp.GetString()
                                       : null;

                if (!root.TryGetProperty("data", out var dataElement) ||
                    !dataElement.TryGetProperty("ressources", out var ressourcesArray) ||
                    ressourcesArray.ValueKind != JsonValueKind.Array)
                {
                    _logger.LogError("Structure JSON invalide : 'data.ressources' manquant ou invalide.");
                    return;
                }

                var dynamicsUrlResource = Environment.GetEnvironmentVariable("DynamicsApiUrlResource");
                var token = await _tokenProvider.GetAccessTokenAsync();
                httpClient.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Bearer", token);
                httpClient.DefaultRequestHeaders.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));


                foreach (var ressource in ressourcesArray.EnumerateArray())
                {
                    try
                    {
                        if (!ressource.TryGetProperty("idressource", out var idressourceProp) ||
                            !idressourceProp.TryGetInt32(out var dc_idressource))
                        {
                            _logger.LogError(
                                "Champ obligatoire 'idressource' manquant ou invalide.");
                            continue;
                        }

                        string firstname =
                            ressource.TryGetProperty("firstname", out var firstnameProp)
                                ? firstnameProp.GetString() ?? ""
                                : "";

                        string contractor =
                            ressource.TryGetProperty("contractor", out var contractoreProp)
                                ? contractoreProp.GetString()
                                : null;

                        string lastname =
                            ressource.TryGetProperty("lastname", out var lastnameProp)
                                ? lastnameProp.GetString() ?? ""
                                : "";

                        string contractorId =
                            ressource.TryGetProperty("idcontractor", out var contractorProp)
                                ? contractorProp.GetString()
                                : null;
                        // Création d'un name sûr
                        string nomComplet = $"{lastname} {firstname}".Trim();
                        nomComplet = $"Ressource-{dc_idressource}";

                        if (!ressource.TryGetProperty("isinternal", out var internalProp) ||
                            internalProp.ValueKind != JsonValueKind.True &&
                                internalProp.ValueKind != JsonValueKind.False)
                        {
                            _logger.LogError("Champ obligatoire 'isinternal' manquant ou invalide.");
                            continue;
                        }
                        bool isinternal = internalProp.GetBoolean();
                        if (!ressource.TryGetProperty("ispda", out var ispdaProp) ||
                            ispdaProp.ValueKind != JsonValueKind.True &&
                                ispdaProp.ValueKind != JsonValueKind.False)
                        {
                            _logger.LogError("Champ obligatoire 'ispda' manquant ou invalide.");
                            continue;
                        }
                        bool ispda = ispdaProp.GetBoolean();

                        if (!ressource.TryGetProperty("isscheduled", out var isscheduledProp) ||
                            (isscheduledProp.ValueKind != JsonValueKind.True &&
                             isscheduledProp.ValueKind != JsonValueKind.False))
                        {
                            _logger.LogError("Champ obligatoire 'isscheduled' manquant ou invalide.");
                            continue;
                        }
                        bool isscheduled = isscheduledProp.GetBoolean();

                        if (!ressource.TryGetProperty("startdate", out var startProp) ||
                            string.IsNullOrWhiteSpace(startProp.GetString()))
                        {
                            _logger.LogError("Champ obligatoire 'startdate' manquant ou vide.");
                            continue;
                        }
                        string dc_DateDebut = startProp.GetString();

                        string dc_DateFin = ressource.TryGetProperty("enddate", out var endProp)
                                                ? endProp.GetString()
                                                : null;
                        if (!ressource.TryGetProperty("idsecteur", out var idsecteurProp) ||
                            !idsecteurProp.TryGetInt32(out var idsecteurVal))
                        {
                            _logger.LogError("Champ obligatoire 'idsecteur' manquant ou invalide.");
                            continue;
                        }

                        if (!ressource.TryGetProperty("matricule", out var matriculeProp) ||
                    string.IsNullOrWhiteSpace(matriculeProp.GetString()))
                        {
                            _logger.LogError("Champ obligatoire 'matricule' manquant ou invalide.");
                            continue;
                        }

                        string matricule = matriculeProp.GetString();

                        int idsecteur = idsecteurVal;
                        string email = ressource.TryGetProperty("email", out var emailProp)
                                           ? emailProp.GetString()
                                           : null;
                        string phoneNumber =
                            ressource.TryGetProperty("phonenumber", out var phoneProp)
                                ? phoneProp.GetString()
                                : null;

                        string addressLine1 = null;
                        string addressLine2 = null;
                        string addressCity = null;
                        string addressPostalCode = null;
                        if (ressource.TryGetProperty("address", out var addressProp) &&
                            addressProp.ValueKind == JsonValueKind.Object)
                        {
                            addressLine1 =
                                addressProp.TryGetProperty("street", out var streetProp)
                                    ? streetProp.GetString()
                                    : null;
                            addressLine2 =
                                addressProp.TryGetProperty("complement", out var street2Prop)
                                    ? street2Prop.GetString()
                                    : null;
                            addressCity = addressProp.TryGetProperty("city", out var cityProp)
                                              ? cityProp.GetString()
                                              : null;
                            addressPostalCode =
                                addressProp.TryGetProperty("postalcode", out var postalProp)
                                    ? postalProp.GetString()
                                    : null;
                        }



                        int? statusValue = null;
                        if (ressource.TryGetProperty("status", out var posJson) &&
                            posJson.ValueKind == JsonValueKind.String)
                        {
                            if (int.TryParse(posJson.GetString(), out int posVal) &&
                                (posVal == 0 || posVal == 1))
                            {
                                statusValue = posVal;
                            }
                        }
                        // si le statut du sous-traitant = inactif alors le sous traitant est
                        // désactivé
                        string contractorGuid = null;
                        if (!string.IsNullOrEmpty(contractorId))
                        {
                            contractorGuid = await GetEntityGuidByField(
                                "accounts", "name", contractorId, "accountid", token, false);
                        }
                        if (contractorGuid != null && statusValue.HasValue &&
                            statusValue.Value == 1)
                        {
                            var disableAccountObj = new Dictionary<string, object> {
                            { "statecode", 1 },  // 1 = Inactive
                            { "statuscode", 2 }
                          };

                            var disableContent =
                                new StringContent(JsonSerializer.Serialize(disableAccountObj), Encoding.UTF8, "application/json");
                            var patchUrlContractor = $"{Environment.GetEnvironmentVariable("DynamicsBaseUrl")}/api/data/v9.2/accounts({contractorGuid})";
                            var patchRequestContractor = new HttpRequestMessage(new HttpMethod("PATCH"), patchUrlContractor) { Content = disableContent };
                            var patchResponseContractor = await httpClient.SendAsync(patchRequestContractor);

                            if (patchResponseContractor.IsSuccessStatusCode)
                                _logger.LogInformation($"Sous-traitant {contractorId} désactivé avec succès.");
                            else
                            {
                                var error = await patchResponseContractor.Content.ReadAsStringAsync();
                                _logger.LogError($"Erreur désactivation sous-traitant {contractorId} : {patchResponseContractor.StatusCode} - {error}");
                            }
                        }
                        if (contractorGuid != null && statusValue.HasValue &&
                            statusValue.Value == 0)
                        {
                            // Récupérer l'état actuel du compte
                            var accountResp = await httpClient.GetAsync(
                                $"{Environment.GetEnvironmentVariable("DynamicsBaseUrl")}/api/data/v9.2/accounts({contractorGuid})?$select=statecode,statuscode");

                            if (accountResp.IsSuccessStatusCode)
                            {
                                var accountJson = await accountResp.Content.ReadAsStringAsync();
                                using var doc = JsonDocument.Parse(accountJson);
                                var stateCode = doc.RootElement.GetProperty("statecode").GetInt32();

                                if (stateCode == 1)  // 1 = inactif, donc on l'active
                                {
                                    var enableAccountObj = new Dictionary<string, object> {
                                    { "statecode", 0 },  // 0 = actif
                                    { "statuscode", 0 }
                                  };

                                    var enableContent =
                                        new StringContent(JsonSerializer.Serialize(enableAccountObj),
                                                          Encoding.UTF8, "application/json");
                                    var patchUrlContractor =
                                        $"{Environment.GetEnvironmentVariable("DynamicsBaseUrl")}/api/data/v9.2/accounts({contractorGuid})";
                                    var patchRequestEnable = new HttpRequestMessage(
                                        new HttpMethod("PATCH"),
                                        patchUrlContractor)
                                    { Content = enableContent };

                                    var patchResponseEnable =
                                        await httpClient.SendAsync(patchRequestEnable);
                                    if (patchResponseEnable.IsSuccessStatusCode)
                                        _logger.LogInformation(
                                            $"Sous-traitant {contractorId} activé avec succès.");
                                    else
                                        _logger.LogError(
                                            $"Erreur activation sous-traitant {contractorId} : {patchResponseEnable.StatusCode} - {await patchResponseEnable.Content.ReadAsStringAsync()}");
                                }
                            }
                            else
                            {
                                _logger.LogError(
                                    $"Impossible de récupérer l'état du sous-traitant {contractorId} : {accountResp.StatusCode}");
                            }
                        }

                        // Résolution du GUID pour idsecteur
                        string secteurGuid = null;

                        secteurGuid = await GetEntityGuidByField("territories", "name", idsecteur, "territoryid", token, false);
                        if (string.IsNullOrEmpty(secteurGuid))
                        {
                            _logger.LogWarning($"Le site avec idsecteur={idsecteur} n'existe pas dans Dynamics.");
                            continue;
                        }

                        // Résolution du GUID de l'utilisateur par firstname + lastname
                        string userGuid = null;

                        if (!string.IsNullOrWhiteSpace(nomComplet))
                        {
                            var queryUser =
                                $"systemusers?$filter=contains(firstname,'{firstname}') and contains(lastname,'{lastname}')&$select=systemuserid";
                            _logger.LogInformation($"Requête utilisateur Dynamics : {queryUser}");

                            var userResp = await httpClient.GetAsync(
                                $"{Environment.GetEnvironmentVariable("DynamicsBaseUrl")}/api/data/v9.2/{queryUser}");
                            if (userResp.IsSuccessStatusCode)
                            {
                                var userJson = await userResp.Content.ReadAsStringAsync();
                                _logger.LogInformation($"Réponse utilisateur : {userJson}");

                                using var docUser = JsonDocument.Parse(userJson);
                                var arr = docUser.RootElement.GetProperty("value");
                                if (arr.GetArrayLength() > 0)
                                {
                                    userGuid = arr[0].GetProperty("systemuserid").GetString();
                                    _logger.LogInformation($"GUID utilisateur trouvé : {userGuid} pour {firstname} {lastname}");
                                }
                                else
                                {
                                    _logger.LogWarning($"Aucun utilisateur trouvé pour {firstname} {lastname}");
                                }
                            }
                            else
                            {
                                _logger.LogError($"Erreur requête utilisateur : {userResp.StatusCode}");
                            }
                        }

                        if (string.IsNullOrEmpty(userGuid))
                        {
                            _logger.LogError(
                                $"Utilisateur {firstname} {lastname} introuvable, ressource ignorée.");
                            continue;
                        }
                        var dynamicsUrl = Environment.GetEnvironmentVariable("DynamicsBaseUrl");

                        // 1️⃣ Mise à jour des champs globaux (email, téléphone, matricule)
                        var userGlobalUpdate = new Dictionary<string, object>();
                        if (!string.IsNullOrWhiteSpace(email))
                            userGlobalUpdate["internalemailaddress"] = email;
                        if (!string.IsNullOrWhiteSpace(phoneNumber))
                            userGlobalUpdate["mobilephone"] = phoneNumber;
                        if (!string.IsNullOrWhiteSpace(matricule))
                            userGlobalUpdate["dc_matricule"] = matricule;

                        // Exécuter le PATCH si au moins un champ global est présent
                        if (userGlobalUpdate.Count > 0)
                        {
                            var userContent =
                                new StringContent(JsonSerializer.Serialize(userGlobalUpdate),
                                                  Encoding.UTF8, "application/json");
                            var patchUrlUser =
                                $"{dynamicsUrl}/api/data/v9.2/systemusers({userGuid})";
                            var patchUserReq = new HttpRequestMessage(
                                new HttpMethod("PATCH"), patchUrlUser)
                            { Content = userContent };
                            await httpClient.SendAsync(patchUserReq);
                        }

                        // 2️⃣ Mise à jour de l'adresse seulement si au moins un champ adresse est
                        // renseigné
                        if (!string.IsNullOrWhiteSpace(addressLine1) ||
                            !string.IsNullOrWhiteSpace(addressLine2) ||
                            !string.IsNullOrWhiteSpace(addressCity) ||
                            !string.IsNullOrWhiteSpace(addressPostalCode))
                        {
                            // Vider les anciennes adresses
                            var clearAddress = new Dictionary<string, object> {
                            { "address1_line1", null },
                            { "address1_line2", null },
                            { "address1_city", null },
                            { "address1_postalcode", null },
                            { "address1_stateorprovince", null },
                            { "address1_country", null }
                          };
                            var clearAddressContent =
                                new StringContent(JsonSerializer.Serialize(clearAddress),
                                                  Encoding.UTF8, "application/json");
                            var clearAddressReq = new HttpRequestMessage(
                                new HttpMethod("PATCH"),
                                $"{dynamicsUrl}/api/data/v9.2/systemusers({userGuid})")
                            {
                                Content = clearAddressContent
                            };
                            await httpClient.SendAsync(clearAddressReq);

                            // Préparer les champs adresse pour la mise à jour
                            var addressUpdate = new Dictionary<string, object>();
                            if (!string.IsNullOrWhiteSpace(addressLine1))
                                addressUpdate["address1_line1"] = addressLine1;
                            if (!string.IsNullOrWhiteSpace(addressLine2))
                                addressUpdate["address1_line2"] = addressLine2;
                            if (!string.IsNullOrWhiteSpace(addressCity))
                                addressUpdate["address1_city"] = addressCity;
                            if (!string.IsNullOrWhiteSpace(addressPostalCode))
                                addressUpdate["address1_postalcode"] = addressPostalCode;

                            var addressContent =
                                new StringContent(JsonSerializer.Serialize(addressUpdate),
                                                  Encoding.UTF8, "application/json");
                            var patchAddressReq = new HttpRequestMessage(
                                new HttpMethod("PATCH"),
                                $"{dynamicsUrl}/api/data/v9.2/systemusers({userGuid})")
                            {
                                Content = addressContent
                            };
                            await httpClient.SendAsync(patchAddressReq);
                        }

                        var ressourceObj = new Dictionary<string, object> {
                          { "resourcetype", 3 },
                          { "dc_idressource", dc_idressource },
                          { "msdyn_displayonscheduleassistant", isscheduled },
                          { "dc_startdate", dc_DateDebut },
                          { "dc_enddate", dc_DateFin },
                          { "dc_isinternal", isinternal },
                          { "dc_ispda", ispda },
                          { "timezone", 105 }
                        };

                        ressourceObj["dc_ispda"] = ispda;
                        if (statusValue.HasValue)
                        {
                            ressourceObj["dc_statutsoustraitant"] = statusValue.Value;
                        }
                        if (!string.IsNullOrEmpty(userGuid))
                            ressourceObj["UserId@odata.bind"] = $"/systemusers({userGuid})";

                        if (!string.IsNullOrEmpty(secteurGuid))
                            ressourceObj["dc_Idsecteur@odata.bind"] =
                                $"/territories({secteurGuid})";

                        // Désactiver si end date dépassée
                        if (!string.IsNullOrEmpty(dc_DateFin) &&
                            DateTime.TryParse(dc_DateFin, out var endDate))
                        {
                            if (endDate < DateTime.UtcNow.Date)
                            {
                                ressourceObj["statecode"] = 1;
                                ressourceObj["statuscode"] = 2;
                            }
                        }
                        // Guid Id contractor
                        // string contractorGuid = null;

                        if (!string.IsNullOrEmpty(contractorId))
                        {
                            // Chercher le contractor existant
                            contractorGuid = await GetEntityGuidByField(
                                "accounts", "name", contractorId, "accountid", token, false);

                            if (string.IsNullOrEmpty(contractorGuid))
                            {
                                _logger.LogWarning(
                                    $"Contractor {contractorId} introuvable. Création dans Dynamics...");
                                var contractorObj = new Dictionary<string, object> {
                                      { "name", contractorId },
                                      { "dc_nomdumobilier", contractor },
                                      { "dc_accounttype", 2 }
                                    };

                                var contentContractor =
                                    new StringContent(JsonSerializer.Serialize(contractorObj),
                                                      Encoding.UTF8, "application/json");
                                var contractorResp = await httpClient.PostAsync(
                                    $"{Environment.GetEnvironmentVariable("DynamicsBaseUrl")}/api/data/v9.2/accounts",
                                    contentContractor);

                                if (contractorResp.IsSuccessStatusCode)
                                {
                                    // Récupérer l'URI de création et extraire le GUID
                                    var contractorUri = contractorResp.Headers.Location;
                                    contractorGuid = contractorUri.Segments.Last().TrimEnd(')');
                                    contractorGuid =
                                        contractorGuid.Substring(contractorGuid.IndexOf('(') + 1);
                                    _logger.LogInformation($"Contractor {contractorId} créé avec GUID {contractorGuid}");
                                }
                                else
                                {
                                    var error = await contractorResp.Content.ReadAsStringAsync();
                                    _logger.LogError($"Erreur création contractor {contractorId} : {contractorResp.StatusCode} - {error}");
                                }
                            }
                        }
                        if (!string.IsNullOrEmpty(contractorGuid))
                        {
                            // Remplacer dc_contractorid par le nom du champ lookup réel dans ta
                            // table bookableresource
                            ressourceObj["dc_Idcontractor@odata.bind"] =
                                $"/accounts({contractorGuid})";
                        }

                        ressourceObj["msdyn_optimizeroute"] = true;
                        HttpResponseMessage ressourceResponse;
                        bool ressourceOk = false;
                        if (eventType == "ressource.updated")
                        {
                            var queryRessourceUrl =
                                $"{dynamicsUrlResource}?$filter=dc_idressource eq {dc_idressource}&$select=bookableresourceid";
                            var getRessourceResponse =
                                await httpClient.GetAsync(queryRessourceUrl);
                            if (getRessourceResponse.IsSuccessStatusCode)
                            {
                                var jsonRessource =
                                    await getRessourceResponse.Content.ReadAsStringAsync();
                                using var docRessource = JsonDocument.Parse(jsonRessource);
                                var valuesRessource = docRessource.RootElement.GetProperty("value");
                                if (valuesRessource.GetArrayLength() > 0)
                                {
                                    var existingRessourceId = valuesRessource[0].GetProperty("bookableresourceid").GetString();
                                    var patchUrl = $"{dynamicsUrlResource}({existingRessourceId})";
                                    var patchRequest = new HttpRequestMessage(
                                        new HttpMethod("PATCH"),
                                        patchUrl)
                                    {
                                        Content = new StringContent(JsonSerializer.Serialize(ressourceObj), Encoding.UTF8, "application/json")
                                    };
                                    ressourceResponse = await httpClient.SendAsync(patchRequest);
                                    if (ressourceResponse.IsSuccessStatusCode)
                                        ressourceOk = true;  // Update réussie
                                }
                                else
                                {
                                    _logger.LogWarning($"Aucune ressource trouvée avec dc_idressource = {dc_idressource}. Création annulée.");
                                    continue;
                                }
                            }
                            else
                            {
                                var error = await getRessourceResponse.Content.ReadAsStringAsync();
                                _logger.LogError($"Erreur recherche ressource : {getRessourceResponse.StatusCode} - {error}");
                                continue;
                            }
                        }
                        else
                        {
                            var contentRessource =
                                new StringContent(JsonSerializer.Serialize(ressourceObj),
                                                  Encoding.UTF8, "application/json");
                            ressourceResponse =
                                await httpClient.PostAsync(dynamicsUrlResource, contentRessource);
                            if (ressourceResponse.IsSuccessStatusCode)
                                ressourceOk = true;  // Création réussie
                        }

                        //   Gestion contractor uniquement si la ressource a été créée ou mise à jour avec succès
                        if (ressourceOk && !string.IsNullOrEmpty(contractorId))
                        {
                            contractorGuid = await GetEntityGuidByField(
                                "accounts", "name", contractorId, "accountid", token, false);

                            if (string.IsNullOrEmpty(contractorGuid))
                            {
                                _logger.LogWarning(
                                    $"Contractor {contractorId} introuvable. Création dans Dynamics...");
                                var contractorObj = new Dictionary<string, object> {
                              { "name", contractorId },
                              { "dc_nomdumobilier", contractor },
                              { "dc_accounttype", 2 }
                            };

                                var contentContractor =
                                    new StringContent(JsonSerializer.Serialize(contractorObj),
                                                      Encoding.UTF8, "application/json");
                                var contractorResp = await httpClient.PostAsync(
                                    $"{Environment.GetEnvironmentVariable("DynamicsBaseUrl")}/api/data/v9.2/accounts",
                                    contentContractor);

                                if (contractorResp.IsSuccessStatusCode)
                                {
                                    var contractorUri = contractorResp.Headers.Location;
                                    contractorGuid = contractorUri.Segments.Last().TrimEnd(')');
                                    contractorGuid =
                                        contractorGuid.Substring(contractorGuid.IndexOf('(') + 1);
                                    _logger.LogInformation(
                                        $"Contractor {contractorId} créé avec GUID {contractorGuid}");
                                }
                                else
                                {
                                    var error = await contractorResp.Content.ReadAsStringAsync();
                                    _logger.LogError(
                                        $"Erreur création contractor {contractorId} : {contractorResp.StatusCode} - {error}");
                                }
                            }

                            if (!string.IsNullOrEmpty(contractorGuid))
                                ressourceObj["dc_Idcontractor@odata.bind"] =
                                    $"/accounts({contractorGuid})";
                        }
                        var dynamicsBaseUrl = Environment.GetEnvironmentVariable("DynamicsBaseUrl");


                        if (ressourceResponse.IsSuccessStatusCode)
                        {
                            // Récupérer la ressource
                            var queryRessourceUrl = $"{dynamicsUrlResource}?$filter=dc_idressource eq {dc_idressource}&$select=bookableresourceid,msdyn_latitude,msdyn_longitude";
                            var getRessourceResponse = await httpClient.GetAsync(queryRessourceUrl);
                            if (!getRessourceResponse.IsSuccessStatusCode)
                            {
                                _logger.LogError($"Erreur récupération ressource : {getRessourceResponse.StatusCode}");
                                return;
                            }

                            var resourceContent = await getRessourceResponse.Content.ReadAsStringAsync();
                            using var docRes = JsonDocument.Parse(resourceContent);
                            var resources = docRes.RootElement.GetProperty("value");
                            if (resources.GetArrayLength() == 0)
                            {
                                _logger.LogWarning($"Aucune bookableresource trouvée pour dc_idressource={dc_idressource}");
                                return;
                            }

                            var res = resources[0];
                            var resId = res.GetProperty("bookableresourceid").GetString();
                            double? lat = res.TryGetProperty("msdyn_latitude", out var latElem) && latElem.ValueKind != JsonValueKind.Null ? latElem.GetDouble() : (double?)null;
                            double? lon = res.TryGetProperty("msdyn_longitude", out var lonElem) && lonElem.ValueKind != JsonValueKind.Null ? lonElem.GetDouble() : (double?)null;

                            // Mettre à jour l'utilisateur si lat/lon manquants
                            if (!lat.HasValue || !lon.HasValue)
                            {
                                var queryUserUrl = $"{dynamicsBaseUrl}/api/data/v9.2/systemusers?$filter=contains(firstname,'{firstname}') and contains(lastname,'{lastname}')&$select=systemuserid,address1_latitude,address1_longitude,address1_line1,address1_city,address1_postalcode";
                                var getUserResponse = await httpClient.GetAsync(queryUserUrl);
                                if (!getUserResponse.IsSuccessStatusCode)
                                {
                                    _logger.LogError($"Erreur récupération utilisateur : {getUserResponse.StatusCode}");
                                    return;
                                }

                                var getUserContent = await getUserResponse.Content.ReadAsStringAsync();
                                using var docUser = JsonDocument.Parse(getUserContent);
                                var users = docUser.RootElement.GetProperty("value");
                                if (users.GetArrayLength() == 0) return;

                                var user = users[0];
                                var userId = user.GetProperty("systemuserid").GetString();

                                var userUpdate = new Dictionary<string, object>();

                                // Adresse ligne 1 avec suppression du premier mot
                                if (user.TryGetProperty("address1_line1", out var addr1) && !string.IsNullOrWhiteSpace(addr1.GetString()))
                                {
                                    var addressLine = addr1.GetString();
                                    var parts = addressLine.Split(' ', 2);
                                    if (parts.Length == 2)
                                        addressLine = parts[1];
                                    userUpdate["address1_line1"] = addressLine;
                                }

                                if (user.TryGetProperty("address1_city", out var addrCity) && !string.IsNullOrWhiteSpace(addrCity.GetString()))
                                    userUpdate["address1_city"] = addrCity.GetString();

                                if (user.TryGetProperty("address1_postalcode", out var addrCode) && !string.IsNullOrWhiteSpace(addrCode.GetString()))
                                    userUpdate["address1_postalcode"] = addrCode.GetString();

                                if (userUpdate.Count > 0)
                                {
                                    var patchUser = new HttpRequestMessage(HttpMethod.Patch, $"{dynamicsBaseUrl}/api/data/v9.2/systemusers({userId})")
                                    {
                                        Content = new StringContent(JsonSerializer.Serialize(userUpdate), Encoding.UTF8, "application/json")
                                    };
                                    await httpClient.SendAsync(patchUser);
                                }
                            }

                            // Mettre à jour la bookableresource
                            var bookableUpdate = new Dictionary<string, object>
        {
            { "msdyn_displayonscheduleboard", true },
            { "msdyn_displayonscheduleassistant", true },
            { "msdyn_startlocation", "690970000" },
            { "msdyn_endlocation", "690970000" },
            { "msdyn_scheduleoutsideworkhours", "192350000" },
            { "msdyn_traveloutsideworkhourslimit", 30 }
        };

                            var patchResource = new HttpRequestMessage(HttpMethod.Patch, $"{dynamicsUrlResource}({resId})")
                            {
                                Content = new StringContent(JsonSerializer.Serialize(bookableUpdate), Encoding.UTF8, "application/json")
                            };
                            await httpClient.SendAsync(patchResource);
                        }

                        else
                        {
                            var error = await ressourceResponse.Content.ReadAsStringAsync();
                            _logger.LogError($"Erreur Dynamics ressource : {ressourceResponse.StatusCode} - {error}");
                        }
                    }
                    catch (Exception ex)
                    {
                        _logger.LogError("Erreur lors du traitement d'une ressource : " +
                                         ex.Message);
                        _logger.LogError(ex.ToString());
                        continue;  // Pour continuer avec les autres ressources
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