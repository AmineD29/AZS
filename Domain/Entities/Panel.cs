// Domain/Entities/Panel.cs
using System.Text.Json.Serialization;

namespace WorkOrderFunctions.Domain.Entities
{
    public class Panel
    {
        [JsonPropertyName("idpanel")] public long IdPanel { get; set; }

        [JsonPropertyName("refpanel")] public string RefPanel { get; set; } = string.Empty;

        // "0" (actif) | "1" (inactif)
        [JsonPropertyName("status")] public string Status { get; set; } = "0";

        [JsonPropertyName("idstructure")] public long IdStructure { get; set; }

        // Type d'accès
        [JsonPropertyName("idtypeacces")] public int IdTypeAcces { get; set; }
        [JsonPropertyName("typeacces")] public string? TypeAcces { get; set; }

        // Type de panel
        [JsonPropertyName("idtypepanel")] public int IdTypePanel { get; set; }
        [JsonPropertyName("typepanel")] public string? TypePanel { get; set; }

        // Surface
        [JsonPropertyName("idarea")] public int IdArea { get; set; }
        [JsonPropertyName("area")] public string? Area { get; set; }

        // Site / territoire
        [JsonPropertyName("idsite")] public int? IdSite { get; set; }

        // Jours / positionnement
        [JsonPropertyName("idstartday")] public int IdStartDay { get; set; }
        [JsonPropertyName("positioning")] public string Positioning { get; set; } = "0";

        // Produit / support (optionnels)
        [JsonPropertyName("product")] public string? Product { get; set; }
        [JsonPropertyName("idproduct")] public int? IdProduct { get; set; }

        [JsonPropertyName("support")] public string? Support { get; set; }
        [JsonPropertyName("idsupport")] public int? IdSupport { get; set; }
    }
}