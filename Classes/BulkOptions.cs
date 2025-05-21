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
    }
}