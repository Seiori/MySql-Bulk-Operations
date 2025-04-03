using Microsoft.EntityFrameworkCore.Metadata;

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
        /// Gets or sets a value indicating whether child entities (defined by navigation collections)
        /// should be recursively updated as part of the bulk operation.
        /// </summary>
        public bool CascadeUpdate { get; set; } = false;

        /// <summary>
        /// Gets or sets the list of child entity navigation property names that should be excluded
        /// from the bulk operation.
        /// </summary>
        public IEnumerable<string> ExcludedChildEntities { get; set; } = new List<string>();
    }
}