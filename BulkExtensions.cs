using System.Reflection;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata;
using MySqlConnector;
using Seiori.MySql.Classes;
using Seiori.MySql.Enums;
using Seiori.MySql.Helpers;

namespace Seiori.MySql
{
    /// <summary>
    /// Provides extension methods for performing bulk operations using EF Core and MySQL.
    /// This includes bulk insert, upsert, update operations as well as recursive processing of child entities.
    /// </summary>
    public static class DbContextExtensions
    {
        /// <summary>
        /// Performs a bulk INSERT operation using a multi‑row INSERT statement.
        /// 
        /// When the bulk options (configured by <paramref name="optionsAction"/>) enable 
        /// <see cref="BulkOptions.SetOutputIdentity"/>, the auto‑generated identity values are retrieved via 
        /// LAST_INSERT_ID() and applied to the inserted entities. If <see cref="BulkOptions.IncludeChildren"/> is true,
        /// any child collections referenced through navigation properties are recursively inserted.
        /// </summary>
        /// <typeparam name="T">The type of the entity being inserted.</typeparam>
        /// <param name="context">The EF Core DbContext used to access the database.</param>
        /// <param name="entities">The collection of entities to insert.</param>
        /// <param name="optionsAction">
        /// A delegate that configures bulk options (such as batch size, identity output, and inclusion of child entities)
        /// via a <see cref="BulkOptions"/> instance.
        /// </param>
        public static async Task BulkInsertAsync<T>(this DbContext context, IEnumerable<T> entities, Action<BulkOptions> optionsAction) where T : class
        {
            var options = new BulkOptions();
            optionsAction(options);
            await BulkOperationAsync(context, entities, options, BulkOperation.Insert).ConfigureAwait(false);
        }

        /// <summary>
        /// Performs a bulk UPSERT operation using a multi‑row INSERT statement with an ON DUPLICATE KEY UPDATE clause.
        /// 
        /// If <paramref name="optionsAction"/> configures <see cref="BulkOptions.SetOutputIdentity"/> to true,
        /// the identity for each row is retrieved individually and applied to the corresponding entity.
        /// If <see cref="BulkOptions.IncludeChildren"/> is true, any child collections are recursively processed 
        /// (and upserted) as well.
        /// </summary>
        /// <typeparam name="T">The type of the entity being upserted.</typeparam>
        /// <param name="context">The EF Core DbContext used to access the database.</param>
        /// <param name="entities">The collection of entities to upsert.</param>
        /// <param name="optionsAction">
        /// A delegate that configures bulk options (such as identity retrieval and child inclusion) via a <see cref="BulkOptions"/> instance.
        /// </param>
        public static async Task BulkUpsertAsync<T>(this DbContext context, IEnumerable<T> entities, Action<BulkOptions> optionsAction) where T : class
        {
            var options = new BulkOptions();
            optionsAction(options);
            await BulkOperationAsync(context, entities, options, BulkOperation.Upsert).ConfigureAwait(false);
        }

        /// <summary>
        /// Performs a bulk UPDATE by generating individual UPDATE statements for each entity.
        /// 
        /// Note that output identity retrieval is not performed for update operations.
        /// The bulk options provided via <paramref name="optionsAction"/> may also control aspects like batch size or
        /// the inclusion of child entities (if any) in the operation.
        /// </summary>
        /// <typeparam name="T">The type of the entity being updated.</typeparam>
        /// <param name="context">The EF Core DbContext used to perform the update operations.</param>
        /// <param name="entities">The collection of entities to update.</param>
        /// <param name="optionsAction">
        /// A delegate to configure bulk options such as batch size or child entity inclusion via a <see cref="BulkOptions"/> instance.
        /// </param>
        public static async Task BulkUpdateAsync<T>(this DbContext context, IEnumerable<T> entities, Action<BulkOptions> optionsAction) where T : class
        {
            var options = new BulkOptions();
            optionsAction(options);
            await BulkOperationAsync(context, entities, options, BulkOperation.Update).ConfigureAwait(false);
        }

        /// <summary>
        /// Executes the specified bulk operation (INSERT, UPSERT, or UPDATE) on a batch of entities.
        /// 
        /// This internal method first validates and retrieves the EF Core model metadata for the entity type, then converts
        /// the input entities into an array. It opens the database connection, splits the entities into batches based on the
        /// batch size from <see cref="BulkOptions"/>, and executes each batch using a dynamically built SQL command.
        /// If <see cref="BulkOptions.SetOutputIdentity"/> is enabled, it retrieves and applies identity values.
        /// If <see cref="BulkOptions.IncludeChildren"/> is true and navigation properties exist, it recursively processes 
        /// child collections.
        /// </summary>
        /// <typeparam name="T">The type of the entity being processed.</typeparam>
        /// <param name="context">The EF Core DbContext used for database operations.</param>
        /// <param name="entities">The collection of entities to process.</param>
        /// <param name="options">
        /// The bulk operation options that control behavior such as batch size, identity retrieval, and recursive child processing.
        /// </param>
        /// <param name="bulkOperation">
        /// The bulk operation type to perform (Insert, Upsert, or Update), specified by the <see cref="BulkOperation"/> enum.
        /// </param>
        private static async Task BulkOperationAsync<T>(DbContext context, IEnumerable<T> entities, BulkOptions options, BulkOperation bulkOperation) where T : class
        {
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

                await BulkExtensionHelpers.ExecuteBulkCommand(context, sql, entityList, entityProps.Properties).ConfigureAwait(false);
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

        /// <summary>
        /// Retrieves and applies the auto‑generated identity values for a batch of entities.
        /// 
        /// After a bulk INSERT or UPSERT operation, this method locates the identity property (configured with
        /// <see cref="ValueGenerated.OnAdd"/>) along with its corresponding lookup columns 
        /// (derived from primary or alternate keys). It then builds and executes a SQL command to retrieve the generated
        /// identity values. The method processes the returned data by matching each identity value to the correct entity,
        /// and assigns the identity value to that entity's identity property.
        /// </summary>
        /// <typeparam name="T">The type of the entity whose identity values are being updated.</typeparam>
        /// <param name="context">The EF Core DbContext used to execute the identity retrieval command.</param>
        /// <param name="entities">The collection of entities for which identity values have been generated.</param>
        /// <param name="entityProps">
        /// The metadata for the entity which includes the table name, key properties, non‑key properties,
        /// and any navigation properties.
        /// </param>
        private static async Task SetIdentityProperties<T>(DbContext context, IEnumerable<T> entities, EntityProperties entityProps)
        {
            if (entityProps.KeyProperties is null) 
                throw new InvalidOperationException($"No Primary Key or Alternate Key found for table {entityProps.TableName}.");
        
            var identityProperty = entityProps.KeyProperties.FirstOrDefault(p => p.ValueGenerated == ValueGenerated.OnAdd);
            if (identityProperty is null) 
                throw new InvalidOperationException($"No Identity Column found for table {entityProps.TableName}.");
        
            var lookupProperties = entityProps.KeyProperties.Except([identityProperty]).ToArray();
            if (lookupProperties.Length == 0) 
                lookupProperties = entityProps.Properties.Except([identityProperty]).ToArray();
            if (lookupProperties.Length == 0) 
                throw new InvalidOperationException($"No Lookup Column found for table {entityProps.TableName}.");
        
            var entityList = entities.ToArray();
            var entityDict = entityList
                .GroupBy(entity => string.Join("|", lookupProperties.Select(p => p.PropertyInfo?.GetValue(entity))))
                .ToDictionary(
                    group => group.Key,
                    group => group.Last() 
                );
            var sql = BuildSqlStatement.BuildSelectIdentitySql(entityProps.TableName, entityList.Length, identityProperty, lookupProperties);
            var parameters = entityList.SelectMany(entity =>
                lookupProperties.Select(property => property.PropertyInfo?.GetValue(entity) ?? DBNull.Value)
            ).ToArray();
        
            await using var cmd = new MySqlCommand(sql, context.Database.GetDbConnection() as MySqlConnection);
        
            cmd.Parameters.AddRange(parameters.Select((p, i) =>
                new MySqlParameter($"@{i}", p)
            ).ToArray());
        
            await cmd.PrepareAsync();
            await using var reader = await cmd.ExecuteReaderAsync();
            while (await reader.ReadAsync())
            {
                var identityValue = reader.GetValue(0);
                var lookupValues = new object[lookupProperties.Length];
                for (var i = 0; i < lookupProperties.Length; i++)
                {
                    var dbValue = reader.GetValue(i + 1);
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
                    identityProperty.PropertyInfo!.SetValue(entity, identityValue);
                }
            }
        }
    }
}