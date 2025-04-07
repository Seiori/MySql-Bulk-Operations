namespace Seiori.MySql.Classes
{
    /// <summary>
    /// Represents options for configuring bulk database operations.
    /// Provides settings to control batching, identity retrieval, and recursive processing of related entities.
    /// </summary>
    public class BulkOptions
    {
        /// <summary>
        /// Gets or sets the number of entities processed in each database command batch.
        /// This affects the size of temporary tables and the number of entities handled per SQL statement (e.g., UPDATE, INSERT SELECT).
        /// Setting this can help manage memory usage and prevent timeout errors for very large datasets.
        /// The default value is 1000.
        /// </summary>
        public int BatchSize { get; set; } = 1000;

        /// <summary>
        /// Gets or sets a value indicating whether server-generated identity values (e.g., from AUTO_INCREMENT columns)
        /// should be retrieved after the operation and updated on the corresponding C# entity objects.
        /// Requires a suitable non-identity key on the entity for matching.
        /// The default value is false.
        /// </summary>
        public bool SetOutputIdentity { get; set; }

        /// <summary>
        /// Gets or sets a value indicating whether related child entities, discovered via navigation properties,
        /// should be recursively processed using the same bulk operation (Insert/Upsert).
        /// If true, all navigable children will be processed unless explicitly excluded via <see cref="ExcludedNavigationPropertyNames"/>.
        /// The default value is false.
        /// </summary>
        public bool IncludeChildEntities { get; set; }

        /// <summary>
        /// Gets or sets a collection of navigation property names (as strings) that should be *excluded*
        /// from recursive processing when <see cref="IncludeChildEntities"/> is true.
        /// This allows skipping specific relationships (e.g., large audit trails, complex sub-graphs handled separately).
        /// The default is an empty collection, meaning no navigation properties are excluded by default.
        /// </summary>
        public IEnumerable<string> ExcludedNavigationPropertyNames { get; set; } = []; // Renamed and default changed
    }
}