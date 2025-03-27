using System.Collections;
using System.Collections.Concurrent;
using System.Data;
using System.Reflection;
using System.Text;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata;
using MySqlConnector;
using Seiori.MySql.Classes;
using Seiori.MySql.Enums;
using Seiori.MySql

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
        var entityType = context.Model.FindEntityType(typeof(T)) ?? throw new InvalidOperationException($"The type {typeof(T).Name} is not part of the EF Core model.");
        
        var entityProps = BulkExtensionHelpers.GetEntityProperties(entityType);
        if (entityProps.TableName is null) throw new InvalidOperationException("Table name could not be determined.");

        // Move to new Functions when Ready :D
        var entityList = entities.ToList();
        var batchedEntities = entityList.Chunk(options.BatchSize).ToList();

        if (context.Database.GetDbConnection().State is not ConnectionState.Open) await context.Database.OpenConnectionAsync();

        var connection = context.Database.GetDbConnection();
        foreach (var batch in batchedEntities)
        {
            var sql = bulkOperation switch
            {
                BulkOperation.Insert => BulkInserter.InsertAsync(entityProps.tableName, entityProps.keyProperties, entityProps.nonKeyProperties, entityProps.allProperties, batch),
                BulkOperation.Upsert => BulkUpserter.UpserterAsync().tableName, entityProps.keyProperties, entityProps.nonKeyProperties, entityProps.allProperties, batch),
                BulkOperation.Update => BulkUpdater.UpdateAsync(entityProps.tableName, entityProps.keyProperties, entityProps.nonKeyProperties, entityProps.allProperties, batch),
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
}