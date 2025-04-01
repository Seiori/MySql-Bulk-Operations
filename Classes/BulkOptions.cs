namespace Seiori.MySql.Classes
{
    /// <summary>
    /// Represents options for configuring bulk operations.
    /// This class provides settings that control how bulk operations are processed,
    /// including batch sizes, retrieval of auto-generated identity values, recursive processing 
    /// of child entities, and specifying which properties to update during bulk update operations.
    /// </summary>
    public class BulkOptions
    {
        /// <summary>
        /// Gets or sets the number of entities to be processed per batch.
        /// The default value is 2500.
        /// </summary>
        public int BatchSize { get; set; } = 2500;

        /// <summary>
        /// Gets or sets a value indicating whether the generated auto‑increment identity value
        /// should be retrieved from the database and applied to the entities after the bulk operation.
        /// </summary>
        public bool SetOutputIdentity { get; set; } = false;

        /// <summary>
        /// Gets or sets a value indicating whether child entities (defined in navigation collections) 
        /// should be recursively processed as part of the bulk operation.
        /// </summary>
        public bool IncludeChildren { get; set; } = false;

        /// <summary>
        /// Gets or sets a dictionary that specifies the properties to update on when performing a bulk update operation.
        /// The key represents the table name, and the value represents a collection of property names to update on that table.
        /// </summary>
        public Dictionary<string, IEnumerable<string>> UpdateOnProperties { get; set; } = new();
    }
}