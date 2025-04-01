using Microsoft.EntityFrameworkCore.Metadata;

namespace Seiori.MySql.Classes
{
    /// <summary>
    /// Represents metadata for an entity used in bulk operations.
    /// This includes the table name, the collection of key properties,
    /// the collection of non‑generated properties, and any navigation properties.
    /// </summary>
    public class EntityProperties
    {
        /// <summary>
        /// Gets the name of the table associated with the entity.
        /// </summary>
        public required string TableName { get; init; }

        /// <summary>
        /// Gets the collection of key properties for the entity.
        /// This typically includes the primary key or alternate keys.
        /// </summary>
        public IEnumerable<IProperty>? KeyProperties { get; init; }

        /// <summary>
        /// Gets the collection of properties for the entity.
        /// These properties are typically the columns used in bulk operations.
        /// </summary>
        public required IEnumerable<IProperty> Properties { get; init; }

        /// <summary>
        /// Gets the collection of navigation properties for the entity.
        /// Navigation properties represent relationships to child entities.
        /// </summary>
        public IEnumerable<INavigation>? NavigationProperties { get; init; }
    }
}