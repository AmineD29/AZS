using System.Text.Json.Serialization;

 namespace WorkOrderFunctions.Domain.Entities
 {
     public sealed class Mobilier
     {
         [JsonPropertyName("idstructure")] public int IdStructure { get; set; }
         [JsonPropertyName("ref")] public string? Ref { get; set; }
         [JsonPropertyName("name")] public string? Name { get; set; }

         [JsonPropertyName("serviceaddress")] public MobilierAddress? ServiceAddress { get; set; }
         [JsonPropertyName("commercialaddress")] public MobilierAddress? CommercialAddress { get; set; }
         [JsonPropertyName("preference")] public MobilierPreference? Preference { get; set; }

         [JsonPropertyName("gamme")] public string? Gamme { get; set; }
         [JsonPropertyName("idgamme")] public int? IdGamme { get; set; }
         [JsonPropertyName("instructions")] public string? Instructions { get; set; }
         // "0" actif, "1" inactif
         [JsonPropertyName("status")] public string? Status { get; set; }

         [JsonPropertyName("latitude")] public double? Latitude { get; set; }
         [JsonPropertyName("longitude")] public double? Longitude { get; set; }
         [JsonPropertyName("commerciallatitude")] public double? CommercialLatitude { get; set; }
         [JsonPropertyName("commerciallongitude")] public double? CommercialLongitude { get; set; }

         [JsonPropertyName("idsite")] public int? IdSite { get; set; }
     }

     public sealed class MobilierAddress
     {
         [JsonPropertyName("street")]     public string? Street { get; set; }
         [JsonPropertyName("complement")] public string? Complement { get; set; }
         [JsonPropertyName("city")]       public string? City { get; set; }
         [JsonPropertyName("postalcode")] public string? PostalCode { get; set; }
     }

     public sealed class MobilierPreference
     {
         [JsonPropertyName("firstOpeningStart")]  public string? FirstOpeningStart { get; set; }
         [JsonPropertyName("firstOpeningEnd")]    public string? FirstOpeningEnd { get; set; }
         [JsonPropertyName("secondOpeningStart")] public string? SecondOpeningStart { get; set; }
         [JsonPropertyName("secondOpeningEnd")]   public string? SecondOpeningEnd { get; set; }
     }
 }