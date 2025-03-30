namespace Seiori.MySql.Classes;

/// <summary>
/// Represents options for bulk operations.
/// </summary>
public class BulkOptions
{
    /// <summary>
    /// Gets or sets the number of entities processed per batch.
    /// </summary>
    public int BatchSize { get; set; } = 500;

    /// <summary>
    /// When true, the generated auto‑increment identity is retrieved and applied to the entities.
    /// </summary>
    public bool SetOutputIdentity { get; set; } = false;

    /// <summary>
    /// When true, child entities (navigation collections) are recursively processed.
    /// </summary>
    public bool IncludeChildren { get; set; } = false;
    
    /// <summary>
    /// Specifies the properties to update on when performing a bulk update operation.
    /// The key specifies the table
    /// The value specifies the properties for that table to update on
    /// <summary>
    public Dictionary<string, IEnumerable<string>> UpdateOnProperties { get; set; } = new();
}