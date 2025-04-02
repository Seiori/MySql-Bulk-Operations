using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata;
using MySqlConnector;
using Seiori.MySql.Classes;

namespace Seiori.MySql.Helpers
{
    public static class BulkExtensionHelpers
    {
        public static EntityProperties GetEntityProperties(IEntityType entityType)
        {
            var tableName = entityType.GetTableName();
            if (string.IsNullOrWhiteSpace(tableName)) throw new InvalidOperationException($"No table name found for table {entityType.GetTableName()}");
            
            var properties = entityType.GetProperties().ToArray();
            if (properties.Length is 0) throw new InvalidOperationException($"No properties found for table {tableName}");
            
            var identityProperty = properties.FirstOrDefault(p => p.ValueGenerated == ValueGenerated.OnAdd);
            var keys = entityType.GetKeys();
            var navigationProperties = entityType.GetNavigations().ToArray();
        
            return new EntityProperties
            {
                TableName = tableName,
                Properties = properties,
                IdentityProperty = identityProperty,
                Keys = keys,
                NavigationProperties = navigationProperties
            };
        }
        
        public static async Task ExecuteBulkNonQueryCommand<T>(
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
    
        public static async Task<List<object[]>> ExecuteBulkReaderAsync<T>(
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

            var results = new List<object[]>();

            await using var reader = await cmd.ExecuteReaderAsync();
            while (await reader.ReadAsync())
            {
                var rowValues = new object[reader.FieldCount];
                reader.GetValues(rowValues);
                results.Add(rowValues);
            }

            return results;
        }
        
        public static async Task<List<object>> ExecuteBulkScalarAsync<T>(
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

            var results = new List<object>();
            
            await using var reader = await cmd.ExecuteReaderAsync();
            while (await reader.ReadAsync())
            {
                results.Add(reader.GetValue(0));
            }

            return results;
        }
        
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
    }
}