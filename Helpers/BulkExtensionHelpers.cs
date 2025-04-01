using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata;
using MySqlConnector;
using Seiori.MySql.Classes;

namespace Seiori.MySql.Helpers
{
    /// <summary>
    /// Provides helper methods for bulk operations, including retrieving entity metadata,
    /// executing bulk SQL commands, updating foreign key values for child entities, and retrieving property values.
    /// </summary>
    public static class BulkExtensionHelpers
    {
        /// <summary>
        /// Retrieves a consolidated set of metadata for the specified entity type needed for bulk operations.
        /// This includes the table name, key properties, non‐key properties (with no value generation), and navigation properties.
        /// </summary>
        /// <param name="entityType">The EF Core entity type from which to extract metadata.</param>
        /// <returns>
        /// An <see cref="EntityProperties"/> object containing the table name, key properties, available properties,
        /// and navigation properties for the entity.
        /// </returns>
        public static EntityProperties GetEntityProperties(IEntityType entityType)
        {
            var tableName = entityType.GetTableName();
            var keyProperties = entityType.GetProperties().Where(p => p.IsKey());
            var properties = entityType.GetProperties().Where(p => p.ValueGenerated == ValueGenerated.Never);
            var navigationProperties = entityType.GetNavigations().ToArray();
        
            return new EntityProperties
            {
                TableName = tableName ?? throw new InvalidOperationException("Table name could not be determined."),
                KeyProperties = keyProperties,
                Properties = properties,
                NavigationProperties = navigationProperties
            };
        }

        /// <summary>
        /// Executes a bulk SQL command using the provided DbContext, SQL statement, and parameters derived from the given entities.
        /// The method builds a flat list of parameters based on the specified properties, configures a <see cref="MySqlCommand"/>,
        /// and executes it.
        /// </summary>
        /// <typeparam name="T">The type of the entity from which parameter values are extracted.</typeparam>
        /// <param name="context">The EF Core DbContext used to obtain the database connection.</param>
        /// <param name="sql">The SQL command to execute.</param>
        /// <param name="entities">The collection of entities used to derive parameter values.</param>
        /// <param name="properties">The entity properties used for parameter extraction.</param>
        /// <returns>A task representing the asynchronous operation.</returns>
        public static async Task ExecuteBulkCommand<T>(
            DbContext context,
            string sql,
            IEnumerable<T> entities,
            IEnumerable<IProperty> properties) where T : class
        {
            var parameters = entities.SelectMany(entity =>
                properties.Select(property => property.PropertyInfo?.GetValue(entity) ?? DBNull.Value)
            ).ToArray();
        
            await using var cmd = new MySqlCommand(sql, context.Database.GetDbConnection() as MySqlConnection);
        
            cmd.Parameters.AddRange(parameters.Select((p, i) =>
                new MySqlParameter($"@{i}", p)
            ).ToArray());
        
            await cmd.PrepareAsync();
            await cmd.ExecuteNonQueryAsync();
        }
    
        /// <summary>
        /// Updates foreign key values on child entities based on their corresponding parent entity keys.
        /// This method retrieves the EF Core navigation metadata and uses the defined foreign key relationship to set dependent
        /// property values on each child entity.
        /// </summary>
        /// <typeparam name="T">The type of the parent entity.</typeparam>
        /// <param name="context">The EF Core DbContext used to retrieve model metadata.</param>
        /// <param name="navProp">The navigation property representing the child collection.</param>
        /// <param name="parentEntities">The collection of parent entities containing children to update.</param>
        public static void UpdateChildForeignKeys<T>(
            DbContext context,
            INavigation navProp,
            IEnumerable<T> parentEntities) where T : class
        {
            var parentEntityList = parentEntities.ToList();
        
            var parentType = parentEntityList[0].GetType();
            var parentEntityType = context.Model.FindEntityType(parentType);

            var navigationMeta = parentEntityType?.FindNavigation(navProp.PropertyInfo!.Name);
            if (navigationMeta == null)
            {
                return;
            }
        
            var foreignKey = navigationMeta.ForeignKey;
            var principalKeyProperties = foreignKey.PrincipalKey.Properties;
            var dependentProperties = foreignKey.Properties;
        
            foreach (var parent in parentEntityList)
            {
                if (navProp.PropertyInfo?.GetValue(parent) is not IEnumerable<object> children)
                {
                    continue;
                }
                foreach (var child in children)
                {
                    for (var i = 0; i < dependentProperties.Count; i++)
                    {
                        var parentKeyValue = principalKeyProperties[i].PropertyInfo?.GetValue(parent);
                        dependentProperties[i].PropertyInfo?.SetValue(child, parentKeyValue);
                    }
                }
            }
        }
    
        /// <summary>
        /// Retrieves the value of the specified property from the given entity.
        /// If the property has an associated value converter, the value is converted to the provider type.
        /// </summary>
        /// <param name="entity">The entity instance from which to retrieve the property value.</param>
        /// <param name="property">The property metadata defining the property to be retrieved.</param>
        /// <returns>
        /// The property value converted to the provider type if a value converter exists; otherwise, the raw property value.
        /// Returns <c>null</c> if the property value is <c>null</c>.
        /// </returns>
        public static object? GetPropertyValue(object entity, IProperty property)
        {
            ArgumentNullException.ThrowIfNull(property);
            ArgumentNullException.ThrowIfNull(entity);
        
            var getter = property.GetGetter();
            var clrValue = getter.GetClrValue(entity);

            if (clrValue is null) return null;
        
            var valueConverter = property.GetValueConverter();
        
            return valueConverter is null ? clrValue : valueConverter.ConvertToProvider(clrValue);
        }
    }
}