using System.Reflection;
using Microsoft.EntityFrameworkCore;
using Seiori.MySql.Classes;
using Seiori.MySql.Enums;
using Seiori.MySql.Helpers;

namespace Seiori.MySql
{
    public static class DbContextExtensions
    {
        public static async Task BulkOperationAsync<T>(this DbContext context, BulkOperation bulkOperation, IEnumerable<T> entities, Action<BulkOptions> optionsAction) where T : class
        {
            var options = new BulkOptions();
            optionsAction(options);

            var entityType = context.Model.FindEntityType(typeof(T)) ?? throw new InvalidOperationException($"The type {typeof(T).Name} is not part of the EF Core model.");
            var entityProps = BulkExtensionHelpers.GetEntityProperties(entityType);
        
            var entityList = entities.ToArray();
            if (entityList.Length == 0) return;
        
            await context.Database.OpenConnectionAsync().ConfigureAwait(false);
        
            var batchedEntities = entityList.Chunk(options.BatchSize).ToArray();
        
            foreach (var batch in batchedEntities)
            {
                var sql = bulkOperation switch
                {
                    BulkOperation.Insert => BuildSqlStatement.BuildInsertSql(entityProps, batch.Length),
                    BulkOperation.Upsert => BuildSqlStatement.BuildUpsertSql(entityProps, batch.Length),
                    BulkOperation.Update => BuildSqlStatement.BuildUpdateSql(entityProps, batch.Length),
                    _ => throw new ArgumentOutOfRangeException(nameof(bulkOperation), bulkOperation, "Unsupported bulk operation type.")
                };

                await BulkExtensionHelpers.ExecuteBulkNonQueryCommand(context, sql, batch, entityProps.Properties).ConfigureAwait(false);
            }
        
            if (options.SetOutputIdentity)
            {
                await SetIdentityProperties(context, entityList, entityProps).ConfigureAwait(false);
                options.SetOutputIdentity = false;
            }
        
            if (options.IncludeChildren && entityProps.NavigationProperties is not null)
            {
                foreach (var navProp in entityProps.NavigationProperties)
                {
                    BulkExtensionHelpers.UpdateChildForeignKeys(context, navProp, entityList);
                
                    var childEntities = entityList
                        .SelectMany(parent =>
                            navProp.PropertyInfo?.GetValue(parent) as IEnumerable<object>
                            ?? [])
                        .ToArray();
                    if (childEntities.Length == 0) continue;
                
                    var childType = navProp.PropertyInfo!.PropertyType;
                    if (childType.IsGenericType) childType = childType.GetGenericArguments()[0];
                
                    var castMethod = typeof(Enumerable)
                        .GetMethod("Cast", BindingFlags.Public | BindingFlags.Static)!
                        .MakeGenericMethod(childType);
                    var typedChildEntities = castMethod.Invoke(null, [childEntities]);
                
                    var bulkMethod = typeof(DbContextExtensions)
                        .GetMethod(nameof(BulkOperationAsync), BindingFlags.NonPublic | BindingFlags.Static)!
                        .MakeGenericMethod(childType);
                
                    await ((Task)bulkMethod.Invoke(null, [context, typedChildEntities, options, bulkOperation])!).ConfigureAwait(false);
                }
            }
        }
        
        private static async Task SetIdentityProperties<T>(DbContext context, IEnumerable<T> entities, EntityProperties entityProps) where T : class
        {
            if (entityProps.IdentityProperty is null) throw new InvalidOperationException($"No Identity Column found for table {entityProps.TableName}.");
            if (entityProps.Keys is null) throw new InvalidOperationException($"No Key Column found for table {entityProps.TableName}.");

            var lookupProperties = entityProps.Keys.FirstOrDefault(k => k.Properties.Contains(entityProps.IdentityProperty) is false)?.Properties;
            if (lookupProperties is null || lookupProperties.Count is 0) throw new InvalidOperationException($"No Lookup Column found for table {entityProps.TableName}.");
            
            var entityList = entities.ToArray();
            var entityDict = entityList
                .GroupBy(entity => string.Join("|", lookupProperties.Select(p => p.PropertyInfo?.GetValue(entity))))
                .ToDictionary(
                    group => group.Key,
                    group => group.Last() 
                );
            var sql = BuildSqlStatement.BuildSelectIdentitySql(entityProps.TableName, entityList.Length, entityProps.IdentityProperty, lookupProperties);
            var results = await BulkExtensionHelpers.ExecuteBulkReaderAsync(context, sql, entityList, entityProps.Properties).ConfigureAwait(false);
            
            foreach (var result in results)
            {
                var identityValue = result.GetValue(0);
                var lookupValues = new object[lookupProperties.Count];
                for (var i = 0; i < lookupProperties.Count; i++)
                {
                    var dbValue = result.GetValue(i + 1);
                    if (dbValue is null) continue;
                    
                    var targetType = lookupProperties[i].ClrType;
            
                    if (targetType.IsEnum is false)
                    {
                        lookupValues[i] = Convert.ChangeType(dbValue, targetType);
                        continue;
                    }
                
                    var enumUnderlyingType = Enum.GetUnderlyingType(targetType);
                    var numericValue = Convert.ChangeType(dbValue, enumUnderlyingType);
                    lookupValues[i] = Enum.ToObject(targetType, numericValue);
                }
            
                var key = string.Join("|", lookupValues);
                if (entityDict.TryGetValue(key, out var entity))
                {
                    entityProps.IdentityProperty.PropertyInfo!.SetValue(entity, identityValue);
                }
            }
        }
    }
}