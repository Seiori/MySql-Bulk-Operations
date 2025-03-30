using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata;
using MySqlConnector;
using Seiori.MySql.Classes;
using Seiori.MySql.Enums;
using Seiori.MySql.Helpers;

namespace Seiori.MySql;

/// <summary>
/// Provides extension methods for performing bulk operations using EF Core and MySQL.
/// </summary>
public static class DbContextExtensions
{
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
        await BulkOperationAsync(context, entities, options, BulkOperation.Insert).ConfigureAwait(false);
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
        await BulkOperationAsync(context, entities, options, BulkOperation.Upsert).ConfigureAwait(false);
    }

    /// <summary>
    /// Performs a bulk UPDATE by generating individual UPDATE statements for each entity.
    /// Note that output identity retrieval is not applicable for update operations.
    /// </summary>
    public static async Task BulkUpdateAsync<T>(this DbContext context, IEnumerable<T> entities, Action<BulkOptions> optionsAction) where T : class
    {
        var options = new BulkOptions();
        optionsAction(options);
        await BulkOperationAsync(context, entities, options, BulkOperation.Update).ConfigureAwait(false);
    }

    /// <summary>
    /// Executes the specified bulk operation on the given entity batch.
    /// Recursively processes child collections if <see cref="BulkOptions.IncludeChildren"/> is true.
    /// </summary>
    private static async Task BulkOperationAsync<T>(DbContext context, IEnumerable<T> entities, BulkOptions options, BulkOperation bulkOperation) where T : class
    {
        var entityType = context.Model.FindEntityType(typeof(T)) ?? throw new InvalidOperationException($"The type {typeof(T).Name} is not part of the EF Core model.");
        var entityProps = BulkExtensionHelpers.GetEntityProperties(entityType);
        
        var entityList = entities.ToArray();
        if (entityList.Length == 0) throw new InvalidOperationException("No entities to process.");
        
        await context.Database.OpenConnectionAsync().ConfigureAwait(false);
        
        var batchedEntities = entityList.Chunk(options.BatchSize).ToArray();
        
        foreach (var batch in batchedEntities)
        {
            switch (bulkOperation)
            {
                case BulkOperation.Insert:
                    await InsertAsync(context, batch, entityProps, options).ConfigureAwait(false);
                    break;
                case BulkOperation.Upsert:
                    await UpsertAsync(context, batch, entityProps, options).ConfigureAwait(false);
                    break;
                case BulkOperation.Update:
                    await UpdateAsync(context, batch, entityProps, options).ConfigureAwait(false);
                    break;
                default:
                    throw new ArgumentOutOfRangeException(nameof(bulkOperation), bulkOperation, "Unsupported bulk operation type.");
            }
        }

        if (options.SetOutputIdentity)
        {
            await SelectIdentityAsync(context, entityList, entityProps, options).ConfigureAwait(false);
        }

        if (options.IncludeChildren && entityProps.NavigationProperties is not null)
        {
            foreach (var navigationProperty in entityProps.NavigationProperties)
            {
                var childEntities = entityList.SelectMany(e => navigationProperty.PropertyInfo?.GetValue(e) as IEnumerable<object> ?? []);
                await BulkOperationAsync(context, childEntities, options, bulkOperation).ConfigureAwait(false);
            }
        }
    }

    private static async Task SelectIdentityAsync<T>(DbContext context, IEnumerable<T> entities, EntityProperties entityProps, BulkOptions options)
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
        var sql = BuildSqlStatement.BuildSelectIdentitySql(entityProps.TableName, entityList.Length, identityProperty, lookupProperties);
        var parameters = BuildSqlStatement.BuildParameters(entityList, lookupProperties);
        
        await using var cmd = new MySqlCommand(sql, context.Database.GetDbConnection() as MySqlConnection);
        
        cmd.Parameters.AddRange(parameters.Select((p, i) =>
            new MySqlParameter($"@{i}", p)
        ).ToArray());
        
        await cmd.PrepareAsync();
        await using var reader = await cmd.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            
        }
    }
    
    private static async Task InsertAsync<T>(DbContext context, IEnumerable<T> entities, EntityProperties entityProps, BulkOptions options) where T : class 
    {
        var entityList = entities.ToArray();
        var sql = BuildSqlStatement.BuildInsertSql(entityProps.TableName, entityList.Length, entityProps.Properties);
        var parameters = BuildSqlStatement.BuildParameters(entityList, entityProps.Properties);
        
        await using var cmd = new MySqlCommand(sql, context.Database.GetDbConnection() as MySqlConnection);
        
        cmd.Parameters.AddRange(parameters.Select((p, i) =>
            new MySqlParameter($"@{i}", p)
        ).ToArray());
        
        await cmd.PrepareAsync();
        await cmd.ExecuteNonQueryAsync();
    }

    private static async Task UpsertAsync<T>(DbContext context, IEnumerable<T> entities, EntityProperties entityProps, BulkOptions options) where T : class
    {
        if (entityProps.KeyProperties is null) throw new InvalidOperationException($"No Primary Key or Alternate Key found for table {entityProps.TableName}.");
        
        var entityList = entities.ToArray();
        var sql = BuildSqlStatement.BuildUpsertSql(entityProps.TableName, entityList.Length, entityProps.Properties);
        var parameters = BuildSqlStatement.BuildParameters(entityList, entityProps.Properties);
        
        await using var cmd = new MySqlCommand(sql, context.Database.GetDbConnection() as MySqlConnection);
        
        cmd.Parameters.AddRange(parameters.Select((p, i) =>
            new MySqlParameter($"@{i}", p)
        ).ToArray());
        
        await cmd.PrepareAsync();
        await cmd.ExecuteNonQueryAsync();
    }
    
    private static async Task UpdateAsync<T>(DbContext context, IEnumerable<T> entities, EntityProperties entityProps, BulkOptions options) where T : class
    {
        var entityList = entities.ToArray();
        var sql = BuildSqlStatement.BuildUpdateSql(entityList, entityProps);
        
        await using var cmd = new MySqlCommand(sql, context.Database.GetDbConnection() as MySqlConnection);
        
        cmd.Parameters.AddRange(BuildSqlStatement.BuildParameters(entityList, entityProps.Properties).Select((p, i) =>
            new MySqlParameter($"@{i}", p)
        ).ToArray());
        
        await cmd.PrepareAsync();
        await cmd.ExecuteNonQueryAsync();
    }
}