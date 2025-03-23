using System.Collections;
using System.Collections.Concurrent;
using System.Data;
using System.Reflection;
using System.Text;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata;
using MySqlConnector;

namespace Seiori.MySql;

/// <summary>
/// Provides extension methods for performing bulk operations using EF Core and MySQL.
/// </summary>
public static class DbContextExtensions
{
    // Cache for reflection lookups for BulkInsertAsync for a given child type.
    private static readonly ConcurrentDictionary<Type, MethodInfo> BulkInsertMethodCache = new();

    #region Bulk Insert / Upsert / Update

    /// <summary>
    /// Performs a bulk INSERT using a multi‑row INSERT statement.
    /// If <paramref name="optionsAction"/> sets <see cref="BulkOptions.SetOutputIdentity"/> to true,
    /// the auto‑generated identity values are retrieved via LAST_INSERT_ID() and applied to the inserted entities.
    /// If <see cref="BulkOptions.IncludeChildren"/> is true, child collections are recursively inserted.
    /// </summary>
    public static async Task BulkInsertAsync<T>(this DbContext context, IEnumerable<T> entities, Action<BulkOptions> optionsAction) where T : class
    {
        var options = new BulkOptions();
        optionsAction(options);
        await BulkOperationAsync(context, entities, options, BulkOperation.Insert);
    }

    /// <summary>
    /// Performs a bulk UPSERT using a multi‑row INSERT statement with an ON DUPLICATE KEY UPDATE clause.
    /// If <paramref name="optionsAction"/> sets <see cref="BulkOptions.SetOutputIdentity"/> to true,
    /// each row’s identity is retrieved individually and applied.
    /// If <see cref="BulkOptions.IncludeChildren"/> is true, child collections are recursively inserted.
    /// </summary>
    public static async Task BulkUpsertAsync<T>(this DbContext context, IEnumerable<T> entities, Action<BulkOptions> optionsAction) where T : class
    {
        var options = new BulkOptions();
        optionsAction(options);
        await BulkOperationAsync(context, entities, options, BulkOperation.Upsert);
    }

    /// <summary>
    /// Performs a bulk UPDATE by generating individual UPDATE statements for each entity.
    /// Note that output identity retrieval is not applicable for update operations.
    /// </summary>
    public static async Task BulkUpdateAsync<T>(this DbContext context, IEnumerable<T> entities, Action<BulkOptions> optionsAction) where T : class
    {
        var options = new BulkOptions();
        optionsAction(options);
        await BulkOperationAsync(context, entities, options, BulkOperation.Update);
    }

    /// <summary>
    /// Executes the specified bulk operation on the given entity batch.
    /// Recursively processes child collections if <see cref="BulkOptions.IncludeChildren"/> is true.
    /// </summary>
    private static async Task BulkOperationAsync<T>(DbContext context, IEnumerable<T> entities, BulkOptions options, BulkOperation bulkOperation) where T : class
    {
        var entityType = context.Model.FindEntityType(typeof(T))
                         ?? throw new InvalidOperationException($"The type {typeof(T).Name} is not part of the EF Core model.");
        var entityProps = GetEntityProperties(entityType);
        if (entityProps.tableName is null)
            throw new InvalidOperationException("Table name could not be determined.");

        var entityList = entities.ToList();
        var batchedEntities = entityList.Chunk(options.BatchSize).ToList();

        if (context.Database.GetDbConnection().State != ConnectionState.Open)
            await context.Database.OpenConnectionAsync();

        var connection = (MySqlConnection)context.Database.GetDbConnection();
        foreach (var batch in batchedEntities)
        {
            var sql = bulkOperation switch
            {
                BulkOperation.Insert =>
                    GetBulkInsertSql(entityProps.tableName, entityProps.keyProperties, entityProps.nonKeyProperties, entityProps.allProperties, batch),
                BulkOperation.Upsert =>
                    GetBulkUpsertSql(entityProps.tableName, entityProps.keyProperties, entityProps.nonKeyProperties, entityProps.allProperties, batch),
                BulkOperation.Update =>
                    GetBulkUpdateSql(entityProps.tableName, entityProps.keyProperties, entityProps.nonKeyProperties, entityProps.allProperties, batch),
                _ => throw new ArgumentOutOfRangeException(nameof(bulkOperation), bulkOperation, "Unsupported bulk operation type.")
            };

            var parameters = BuildParameters(batch, entityProps.allProperties);
            await using var command = new MySqlCommand(sql, connection);
            command.Parameters.AddRange(parameters.Select((p, i) =>
                new MySqlParameter($"@{i}", p)
            ).ToArray());
            
            await command.PrepareAsync();

            await command.ExecuteNonQueryAsync();

            if (options.SetOutputIdentity && (bulkOperation == BulkOperation.Insert || bulkOperation == BulkOperation.Upsert))
            {
                var primaryKeyProperties = entityType.FindPrimaryKey()?.Properties
                                           ?? throw new InvalidOperationException($"The type {typeof(T).Name} does not have a primary key.");
                var autoGeneratedKey = primaryKeyProperties.FirstOrDefault(p => p.ValueGenerated == ValueGenerated.OnAdd);
                if (autoGeneratedKey != null)
                {
                    switch (bulkOperation)
                    {
                        case BulkOperation.Insert:
                        {
                            var firstId = Convert.ToInt32(command.LastInsertedId);
                            for (var index = 0; index < batch.Length; index++)
                            {
                                autoGeneratedKey.PropertyInfo!.SetValue(batch[index],
                                    Convert.ChangeType(firstId + index, autoGeneratedKey.ClrType));
                            }

                            break;
                        }
                        case BulkOperation.Upsert:
                            var updatedCount = 0;
                        {
                            var allKeys = entityType.GetKeys().ToList();
                            var alternateKey = allKeys.FirstOrDefault(k =>
                                !k.Properties.SequenceEqual(entityType.FindPrimaryKey()!.Properties) &&
                                k.Properties.All(p => p.PropertyInfo!.GetValue(batch.First()) is not null));
                            if (alternateKey == null)
                                throw new InvalidOperationException("BulkUpsert with SetOutputIdentity requires an alternate key with non-null values to retrieve identity values.");

                            var selectQuery = GenerateSelectStatement(batch, entityProps, alternateKey.Properties.ToList());
                            command.CommandText = selectQuery;
                            command.Parameters.Clear();
                            command.Parameters.AddRange(BuildLookupParameters(batch, alternateKey.Properties));
                            await using var reader = (MySqlDataReader)await command.ExecuteReaderAsync();

                            var entityDictionary = batch.ToDictionary(
                                entity => string.Join("|", alternateKey.Properties.Select(p => p.PropertyInfo!.GetValue(entity)?.ToString() ?? ""))
                            );

                            while (reader.Read())
                            {
                                var dbId = reader[autoGeneratedKey.GetColumnName()];
                                var compositeKey = string.Join("|", alternateKey.Properties.Select(p => reader[p.GetColumnName()]?.ToString() ?? ""));
                                if (!entityDictionary.TryGetValue(compositeKey, out var entity)) continue;
                                autoGeneratedKey.PropertyInfo!.SetValue(entity,
                                    Convert.ChangeType(dbId, autoGeneratedKey.ClrType));
                                updatedCount++;
                            }
                            if (updatedCount != batch.Length)
                                throw new InvalidOperationException("Not every entity was updated with a proper identity value during UPSERT.");
                            break;
                        }
                        case BulkOperation.Update:
                            break;
                        default:
                            throw new ArgumentOutOfRangeException(nameof(bulkOperation), bulkOperation, null);
                    }
                }
            }

            if (options.IncludeChildren) await ProcessChildEntitiesAsync(context, batch, options);
        }
    }

    #endregion

    #region Process Child Entities

    /// <summary>
    /// Processes child entities of the parent entities via navigation properties.
    /// If <see cref="BulkOptions.SetOutputIdentity"/> is true, each child's foreign keys are updated
    /// to match the parent's new key before recursively bulk inserting the children.
    /// </summary>
    private static async Task ProcessChildEntitiesAsync<T>(DbContext context, IEnumerable<T> parentEntities, BulkOptions options)
    {
        var entityType = context.Model.FindEntityType(typeof(T)) ?? throw new InvalidOperationException($"No entity type information found for {nameof(T)}.");

        foreach (var navigation in entityType.GetNavigations().Where(n => n.IsCollection))
        {
            if (options.SetOutputIdentity)
            {
                foreach (var parent in parentEntities)
                    UpdateForeignKeysForNavigation(parent, navigation);
            }

            var children = new List<object>();
            foreach (var parent in parentEntities)
            {
                if (navigation.PropertyInfo.GetValue(parent) is not IEnumerable childCollection) continue;
                children.AddRange(childCollection.Cast<object>());
            }

            if (children.Count <= 0) continue;
            var childType = navigation.PropertyInfo.PropertyType
                .GetInterfaces()
                .FirstOrDefault(i => i.IsGenericType && i.GetGenericTypeDefinition() == typeof(IEnumerable<>))
                ?.GetGenericArguments()[0] ?? navigation.TargetEntityType.ClrType;

            var genericMethod = BulkInsertMethodCache.GetOrAdd(childType, t =>
            {
                var method = typeof(DbContextExtensions).GetMethod(nameof(BulkInsertAsync));
                if (method is null)
                    throw new InvalidOperationException("BulkInsertAsync method not found.");
                return method.MakeGenericMethod(t);
            });

            var castMethod = typeof(Enumerable).GetMethod("Cast")!.MakeGenericMethod(childType);
            var typedChildren = castMethod.Invoke(null, [children]);
            var toListMethod = typeof(Enumerable).GetMethod("ToList")!.MakeGenericMethod(childType);
            var finalChildren = toListMethod.Invoke(null, [typedChildren]);

            var task = (Task)genericMethod.Invoke(null, [context, finalChildren, (Action<BulkOptions>)ChildOptions])!;
            await task.ConfigureAwait(false);
            continue;

            void ChildOptions(BulkOptions o)
            {
                o.BatchSize = options.BatchSize;
                o.SetOutputIdentity = options.SetOutputIdentity;
                o.IncludeChildren = options.IncludeChildren;
            }
        }
    }

    /// <summary>
    /// Updates the foreign key properties on each child in the given navigation collection for the parent.
    /// </summary>
    private static void UpdateForeignKeysForNavigation(object parent, INavigation navigation)
    {
        var foreignKey = navigation.ForeignKey;
        var dependentProperties = foreignKey.Properties;
        var principalProperties = foreignKey.PrincipalKey.Properties;
        var pkValues = principalProperties.Select(p => p.PropertyInfo!.GetValue(parent)).ToList();
        if (pkValues.Any(v => v is null))
            return;
        if (navigation.PropertyInfo.GetValue(parent) is not IEnumerable childCollection) return;
        foreach (var child in childCollection)
        {
            var i = 0;
            foreach (var depProp in dependentProperties)
            {
                depProp.PropertyInfo!.SetValue(child, pkValues[i]!);
                i++;
            }
        }
    }

    #endregion

    #region SQL Generators

    private static string GetBulkInsertSql<T>(string tableName, List<IProperty> keyProperties, List<IProperty> nonKeyProperties, List<IProperty> allProperties, IEnumerable<T> entities)
    {
        var baseSql = BuildBulkInsertSqlBase(tableName, allProperties, entities);
        return baseSql + ";";
    }

    private static string GetBulkUpsertSql<T>(string tableName, List<IProperty> keyProperties, List<IProperty> nonKeyProperties, List<IProperty> allProperties, IEnumerable<T> entities)
    {
        var baseSql = BuildBulkInsertSqlBase(tableName, allProperties, entities);
        var updateClause = " ON DUPLICATE KEY UPDATE " +
                           string.Join(", ", nonKeyProperties.Select(p => $"{p.GetColumnName()} = VALUES({p.GetColumnName()})"));
        return baseSql + updateClause + ";";
    }

    private static string GetBulkUpdateSql<T>(string tableName, List<IProperty> keyProperties, List<IProperty> nonKeyProperties, List<IProperty> allProperties, IEnumerable<T> entities)
    {
        var entityList = entities.ToList();
        var setClause = string.Join(", ", nonKeyProperties.Select(p => $"{p.GetColumnName()} = VALUES({p.GetColumnName()})"));
        var whereClause = BuildWhereClause(keyProperties, entityList);
        return $"UPDATE {tableName} SET {setClause} WHERE {whereClause};";
    }

    private static string BuildBulkInsertSqlBase<T>(string tableName, List<IProperty> allProperties, IEnumerable<T> entities)
    {
        var entityList = entities.ToList();
        var rowCount = entityList.Count;
        var colCount = allProperties.Count;
        var sb = new StringBuilder();
        sb.Append($"INSERT INTO {tableName} (");
        sb.Append(string.Join(", ", allProperties.Select(p => p.GetColumnName())));
        sb.Append(") VALUES ");
        var rows = new string[rowCount];
        for (var i = 0; i < rowCount; i++)
        {
            var placeholders = new string[colCount];
            for (var j = 0; j < colCount; j++)
            {
                placeholders[j] = $"@{i * colCount + j}";
            }
            rows[i] = $"({string.Join(", ", placeholders)})";
        }
        sb.Append(string.Join(", ", rows));
        return sb.ToString();
    }

    private static string BuildWhereClause<T>(List<IProperty> keyProperties, List<T> entities)
    {
        var clauses = new string[entities.Count];
        for (var i = 0; i < entities.Count; i++)
        {
            var conditions = keyProperties.Select(p => $"{p.GetColumnName()} = @{i * keyProperties.Count + keyProperties.IndexOf(p)}");
            clauses[i] = $"({string.Join(" AND ", conditions)})";
        }
        return string.Join(" OR ", clauses);
    }

    private static string GenerateSelectStatement<T>(IEnumerable<T> entities, (string? tableName, List<IProperty> keyProperties, List<IProperty> nonKeyProperties, List<IProperty> allProperties) entityProps, List<IProperty> lookupProperties)
    {
        var primaryKeyName = entityProps.keyProperties[0].GetColumnName();
        var sb = new StringBuilder();
        sb.Append("SELECT ");
        sb.Append(primaryKeyName);
        foreach (var prop in lookupProperties)
        {
            sb.Append(", ");
            sb.Append(prop.GetColumnName());
        }
        sb.Append(" FROM ");
        sb.Append(entityProps.tableName);
        sb.Append(" WHERE ");
        var paramIndex = 0;
        var conditionList = entities.Select(_ => lookupProperties.Select(p => $"{p.GetColumnName()} = @p{paramIndex++}")).Select(conditions => $"({string.Join(" AND ", conditions)})").ToList();
        sb.Append(string.Join(" OR ", conditionList));
        sb.Append(';');
        return sb.ToString();
    }

    #endregion

    #region Helpers

    private static object[] BuildParameters<T>(IEnumerable<T> entities, List<IProperty> allProperties)
    {
        return entities.SelectMany(entity =>
            allProperties.Select(property => property.PropertyInfo?.GetValue(entity) ?? DBNull.Value)
        ).ToArray();
    }

    private static MySqlParameter[] BuildLookupParameters<T>(IEnumerable<T> entities, IEnumerable<IProperty> lookupProperties)
    {
        var parameters = new List<MySqlParameter>();
        foreach (var entity in entities)
        {
            foreach (var prop in lookupProperties)
            {
                var value = prop.PropertyInfo?.GetValue(entity) ?? DBNull.Value;
                parameters.Add(new MySqlParameter($"@p{parameters.Count}", value));
            }
        }
        return parameters.ToArray();
    }

    private static (string? tableName, List<IProperty> keyProperties, List<IProperty> nonKeyProperties, List<IProperty> allProperties) GetEntityProperties(IEntityType entityType)
    {
        var tableName = entityType.GetTableName();
        var keyProperties = entityType.FindPrimaryKey()?.Properties.ToList() ?? [];
        var allProperties = entityType.GetProperties()
            .Where(p => keyProperties.Contains(p) || p.ValueGenerated == ValueGenerated.Never)
            .ToList();
        var nonKeyProperties = allProperties.Except(keyProperties).ToList();
        return (tableName, keyProperties, nonKeyProperties, allProperties);
    }

    #endregion
}

/// <summary>
/// Represents options for bulk operations.
/// </summary>
public class BulkOptions
{
    /// <summary>
    /// Gets or sets the number of entities processed per batch.
    /// </summary>
    public int BatchSize { get; set; } = 500;

    /// <summary>
    /// When true, the generated auto‑increment identity is retrieved and applied to the entities.
    /// </summary>
    public bool SetOutputIdentity { get; set; } = false;

    /// <summary>
    /// When true, child entities (navigation collections) are recursively processed.
    /// </summary>
    public bool IncludeChildren { get; set; } = false;
}

/// <summary>
/// Enumerates the types of bulk operations.
/// </summary>
public enum BulkOperation
{
    Insert,
    Upsert,
    Update
}