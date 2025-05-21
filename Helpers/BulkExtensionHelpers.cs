using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata;
using Seiori.MySql.Classes;

namespace Seiori.MySql.Helpers
{
    public static class BulkExtensionHelpers
    {
        public static EntityProperties GetEntityProperties(
            IEntityType entityType
            )
        {
            var tableName = entityType.GetTableName();
            if (string.IsNullOrWhiteSpace(tableName)) throw new InvalidOperationException($"No table name found");
            
            var properties = entityType.GetProperties().Where(p => p.ValueGenerated is ValueGenerated.Never).ToArray();
            if (properties.Length is 0) throw new InvalidOperationException($"No properties found for table {tableName}");
            
            var identityProperty = entityType.GetProperties().FirstOrDefault(p => p.ValueGenerated is ValueGenerated.OnAdd);
            
            var keyProperties = entityType.GetKeys().FirstOrDefault(k => k.Properties.All(p => p.ValueGenerated is ValueGenerated.Never))?.Properties;
            if (keyProperties is null || keyProperties.Count is 0) throw new InvalidOperationException($"No key properties found for table {tableName}. Excluding the identity property.");

            return new EntityProperties
            {
                TableName = tableName,
                Properties = properties,
                IdentityProperty = identityProperty,
                KeyProperties = keyProperties,
            };
        }
        
        public static void UpdateChildForeignKeys<T>(
            DbContext context,
            INavigation navProp,
            IEnumerable<T> parentEntities
            ) where T : class
        {
            var parentEntityList = parentEntities.ToList();
        
            var parentType = parentEntityList[0].GetType();
            var parentEntityType = context.Model.FindEntityType(parentType);

            var navigationMeta = parentEntityType?.FindNavigation(navProp.PropertyInfo!.Name);
            if (navigationMeta == null) return;
        
            var foreignKey = navigationMeta.ForeignKey;
            var principalKeyProperties = foreignKey.PrincipalKey.Properties;
            var dependentProperties = foreignKey.Properties;
        
            foreach (var parent in parentEntityList)
            {
                if (navProp.PropertyInfo?.GetValue(parent) is not IEnumerable<object> children) continue;
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
    }
}