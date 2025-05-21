using System.Data;
using System.Text;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata;
using MySqlConnector;
using Seiori.MySql.Classes;
using Seiori.MySql.Enums;
using Seiori.MySql.Helpers;

namespace Seiori.MySql
{
    public static class DbContextExtensions
    {
        public static async Task BulkOperationAsync<T>(
            this DbContext context, 
            BulkOperation bulkOperation, 
            IEnumerable<T> entities, 
            Action<BulkOptions>? optionsAction = null
            ) where T : class
        {
            var options = new BulkOptions();

            optionsAction?.Invoke(options);

            var entityList = entities.ToArray();
            if (entityList.Length == 0) return;
            
            var entityType = context.Model.FindEntityType(typeof(T)) ?? throw new InvalidOperationException($"The type {typeof(T).Name} is not part of the EF Core model.");
            var entityProperties = BulkExtensionHelpers.GetEntityProperties(entityType);
            
            var entityKeyEqualityComparer = new EntityKeyEqualityComparer<T>(entityProperties.KeyProperties);
            var distinctEntityList = entityList.Distinct(entityKeyEqualityComparer).ToArray();
            
            await context.Database.OpenConnectionAsync().ConfigureAwait(false);
            
            var tempTableName = await CreateAndLoadTempTable(context, entityProperties.TableName, entityProperties.Properties, entityProperties.KeyProperties, distinctEntityList).ConfigureAwait(false);
            var tempTableNameQuoted = $"`{tempTableName}`";
            
            try
            {
                switch (bulkOperation)
                {
                    case BulkOperation.Insert:
                        await Insert(context, entityProperties, tempTableNameQuoted).ConfigureAwait(false);
                        break;
                    case BulkOperation.Upsert:
                        await Upsert(context, entityProperties, tempTableNameQuoted).ConfigureAwait(false);
                        break;
                    case BulkOperation.Update:
                    default:
                        throw new NotSupportedException($"Bulk operation {bulkOperation} is not supported.");
                }
                
                if (options.SetOutputIdentity && entityProperties.IdentityProperty is not null)
                {
                    await SetOutputIdentities(context, entityProperties, tempTableName, entityList).ConfigureAwait(false);
                }
            }
            catch (Exception e)
            {
                Console.WriteLine(e);
                throw;
            }
            finally
            {
                var dropTempTableSql = $"DROP TEMPORARY TABLE IF EXISTS `{tempTableName}`;";
                await context.Database.ExecuteSqlRawAsync(dropTempTableSql).ConfigureAwait(false);
            }
            
            if (options.SetOutputIdentity && entityProperties.IdentityProperty is not null)
            {
                foreach (var navProp in entityType.GetNavigations())
                {
                    BulkExtensionHelpers.UpdateChildForeignKeys(context, navProp, entityList); 
                }
            }
        }

        private static async Task Insert(
            DbContext context,
            EntityProperties entityProperties,
            string tempTableName
            )
        {
            var mainTableName = $"`{entityProperties.TableName}`";

            var allColumnNames = entityProperties.Properties
                .Select(p => $"`{p.GetColumnName()}`")
                .ToArray();

            var insertColumnNames = string.Join(", ", allColumnNames);
            var selectColumnNames = string.Join(", ", allColumnNames.Select(c => $"temp.{c}"));
            var sbInsert = new StringBuilder();
            sbInsert.Append($"INSERT IGNORE INTO {mainTableName} ({insertColumnNames}) ");
            sbInsert.Append($"SELECT {selectColumnNames} FROM {tempTableName} AS temp;");

            await context.Database.ExecuteSqlRawAsync(sbInsert.ToString()).ConfigureAwait(false);
        }
        
        private static async Task Upsert(
            DbContext context,
            EntityProperties entityProperties,
            string tempTableName
            )
        {
            var mainTableName = $"`{entityProperties.TableName}`";
            var keyProperties = entityProperties.KeyProperties.ToArray();
            var keyColumnNames = keyProperties.Select(p => $"`{p.GetColumnName()}`").ToArray();
            var allColumnNames = entityProperties.Properties
                .Select(p => $"`{p.GetColumnName()}`")
                .ToArray();
            var updateColumnNames = entityProperties.Properties
                .Where(p => !keyProperties.Contains(p))
                .Select(p => $"`{p.GetColumnName()}`")
                .ToArray();

            if (updateColumnNames.Length is not 0)
            {
                var joinCondition = string.Join(" AND ", keyColumnNames.Select(k => $"main.{k} <=> temp.{k}"));
                var updateSetClause = string.Join(", ", updateColumnNames.Select(col => $"main.{col} = temp.{col}"));
            
                var sbUpdate = new StringBuilder();
                sbUpdate.Append($"UPDATE {mainTableName} AS main ");
                sbUpdate.Append($"JOIN {tempTableName} AS temp ON {joinCondition} ");
                sbUpdate.Append($"SET {updateSetClause};");
                await context.Database.ExecuteSqlRawAsync(sbUpdate.ToString()).ConfigureAwait(false);   
            }

            var insertJoinCondition = string.Join(" AND ", keyColumnNames.Select(k => $"main.{k} <=> temp.{k}"));
            var sbInsert = new StringBuilder();
            sbInsert.Append($"INSERT INTO {mainTableName} ({string.Join(", ", allColumnNames)}) ");
            sbInsert.Append($"SELECT {string.Join(", ", allColumnNames.Select(c => $"temp.{c}"))} ");
            sbInsert.Append($"FROM {tempTableName} AS temp ");
            sbInsert.Append($"LEFT JOIN {mainTableName} AS main ON {insertJoinCondition} ");
            sbInsert.Append($"WHERE main.{keyColumnNames.First()} IS NULL;");

            await context.Database.ExecuteSqlRawAsync(sbInsert.ToString()).ConfigureAwait(false);
        }

        private static DataTable ConvertEntitiesToDataTable<T>(
            string tableName,
            IEnumerable<IProperty> properties,
            IEnumerable<T> entities
            ) where T : class
        {
            var propertyList = properties.ToArray();
            
            var table = new DataTable(tableName);
            foreach (var property in propertyList)
            {
                var columnType = property.ClrType;
                if (property.IsNullable)
                {
                    columnType = Nullable.GetUnderlyingType(columnType) ?? columnType;
                }
                
                table.Columns.Add(property.GetColumnName(), columnType);
            }
            
            foreach (var entity in entities)
            {
                var row = table.NewRow();
                foreach (var property in propertyList)
                {
                    var value = property.PropertyInfo?.GetValue(entity);
                    if (value is null && property.IsNullable is false)
                    {
                        throw new InvalidOperationException($"Property {property.GetColumnName()} cannot be null.");
                    }
                    
                    row[property.GetColumnName()] = value ?? DBNull.Value;
                }
                
                table.Rows.Add(row);
            }
            
            return table;
        }

        private static async Task<string> CreateAndLoadTempTable<T>(
            DbContext context,
            string tableName,
            IEnumerable<IProperty> properties,
            IEnumerable<IProperty> keyProperties,
            IEnumerable<T> entities
            ) where T : class
        {
            var tempTableName = $"Temp_{tableName}_{Guid.NewGuid():N}";
            var propertyList = properties.ToArray();
            var keyPropertyList = keyProperties.ToArray();
            
            var sbCreate = new StringBuilder();
            sbCreate.Append($"CREATE TEMPORARY TABLE `{tempTableName}` (");
            sbCreate.Append(string.Join(", ", propertyList.Select(p => $"`{p.GetColumnName()}` {p.GetColumnType()}")));
            if (keyPropertyList.Length is not 0)
            {
                sbCreate.Append($", PRIMARY KEY ({string.Join(", ", keyPropertyList.Select(p => $"`{p.GetColumnName()}`"))})");
            }
            sbCreate.Append(") ENGINE=MEMORY;");
            
            await context.Database.ExecuteSqlRawAsync(sbCreate.ToString()).ConfigureAwait(false);
            
            var table = ConvertEntitiesToDataTable(tempTableName, propertyList, entities);
            
            if (table.Rows.Count is 0) return tempTableName;
            
            var connection = context.Database.GetDbConnection();
            var bulkCopy = new MySqlBulkCopy((MySqlConnection)connection)
            {
                DestinationTableName = tempTableName
            };
            
            foreach (DataColumn column in table.Columns)
            {
                bulkCopy.ColumnMappings.Add(new MySqlBulkCopyColumnMapping(table.Columns.IndexOf(column), column.ColumnName));
            }
            await bulkCopy.WriteToServerAsync(table).ConfigureAwait(false);

            return tempTableName;
        }

        private static async Task SetOutputIdentities<T>(
            DbContext context,
            EntityProperties entityProperties,
            string tempTableName,
            IEnumerable<T> entities
            ) where T : class
        {
            var identityProperty = entityProperties.IdentityProperty;
            if (identityProperty is null) throw new InvalidOperationException($"No identity property found for table {entityProperties.TableName}.");
            
            var keyPropertyList = entityProperties.KeyProperties.ToArray();
            var entityList = entities.ToArray();
            var entityLookup = entityList
                .ToLookup(
                    e => string.Join("|", keyPropertyList.Select(p =>
                    {
                        var value = p.PropertyInfo?.GetValue(e);
                        return value?.ToString() ?? "NULL";
                    })),
                    e => e
                );
            var keyColumnNames = keyPropertyList.Select(p => $"`{p.GetColumnName()}`").ToArray();
            
            var sbSelect = new StringBuilder();
            sbSelect.Append($"SELECT main.`{identityProperty.GetColumnName()}`");
            foreach (var keyColName in keyColumnNames)
            {
                sbSelect.Append($", temp.{keyColName}");
            }
            sbSelect.Append($" FROM `{tempTableName}` AS temp");
            sbSelect.Append($" INNER JOIN `{entityProperties.TableName}` AS main ON ");
            var joinConditions = keyColumnNames.Select(k => $"main.{k} <=> temp.{k}");
            sbSelect.Append(string.Join(" AND ", joinConditions));
            sbSelect.Append(';');
            
            await using var cmd = context.Database.GetDbConnection().CreateCommand();
            cmd.CommandText = sbSelect.ToString();
            await using var reader = await cmd.ExecuteReaderAsync().ConfigureAwait(false);
            while (await reader.ReadAsync().ConfigureAwait(false))
            {
                var identityValue = reader.GetValue(0);
                var lookupValues = new object[keyColumnNames.Length];
                
                for (var i = 0; i < keyColumnNames.Length; i++)
                {
                    var dbValue = reader.GetValue(i + 1);
                
                    var targetType = keyPropertyList[i].ClrType;
        
                    if (targetType.IsEnum)
                    {
                        lookupValues[i] = Enum.ToObject(targetType, dbValue);
                    }
                    else
                    {
                        lookupValues[i] = Convert.ChangeType(dbValue, targetType);
                    }
                }
                
                var key = string.Join("|", lookupValues.Select(lv => lv.ToString() ?? "NULL"));
                var identityTargetType = identityProperty.ClrType;
                var convertedIdentityValue = Convert.ChangeType(identityValue, identityTargetType);

                foreach (var entity in entityLookup[key])
                {
                    identityProperty.PropertyInfo?.SetValue(entity, convertedIdentityValue);
                }
            }
        }
    }
}