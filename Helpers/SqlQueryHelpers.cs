using System.Text;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata;
using MySqlConnector;
using Seiori.MySql.Classes; // Assuming this contains EntityProperties

namespace Seiori.MySql.Helpers
{
    public static class SqlQueryHelpers
    {
        #region Query Helpers

        public static async Task<IEnumerable<object[]>> SelectIdentity<T>(
            DbContext context,
            string tableName,
            IProperty identityProperty,
            IEnumerable<IProperty> lookupProperties,
            IEnumerable<T> entities
            ) where T : class
        {
            var guid = Guid.NewGuid().ToString("N");
            var tempTableName = $"`{tableName}_{guid}`";
            var entityList = entities.ToArray();
            var lookupPropertyList = lookupProperties.ToArray();
            var identityColumnName = $"`{identityProperty.GetColumnName()}`";
            var lookupColumnNames = lookupPropertyList
                .Select(p => $"`{p.GetColumnName()}`")
                .ToArray();
            var lookupColumnTypes = lookupPropertyList
                .Select(p => p.GetColumnType())
                .ToArray();
            
            var sbCreate = new StringBuilder();
            sbCreate.Append($"CREATE TEMPORARY TABLE {tempTableName} (");
            sbCreate.Append($"{identityColumnName} {identityProperty.GetColumnType()} NULL, ");
            sbCreate.Append(string.Join(", ", lookupColumnNames.Select((name, i) => $"{name} {lookupColumnTypes[i]}")));
            sbCreate.Append(", ");
            sbCreate.Append($"UNIQUE KEY ({string.Join(", ", lookupColumnNames)})");
            sbCreate.Append(");");

            await ExecuteNonQuery(context, sbCreate.ToString()).ConfigureAwait(false);
            
            var sbInsert = new StringBuilder();
            sbInsert.Append($"INSERT IGNORE INTO {tempTableName} (");
            sbInsert.Append(string.Join(", ", lookupColumnNames));
            sbInsert.Append($") VALUES ");
            sbInsert.Append(GetPlaceholders(entityList.Length, lookupPropertyList.Length));
            sbInsert.Append(';');

            await ExecuteNonQueryWithParameters(context, sbInsert.ToString(), lookupPropertyList, entityList).ConfigureAwait(false);
            
            var sbUpdate = new StringBuilder();
            sbUpdate.Append($"UPDATE {tempTableName} AS temp ");
            sbUpdate.Append($"JOIN `{tableName}` AS main ON ");
            sbUpdate.Append(string.Join(" AND ", lookupColumnNames.Select(colName => $"temp.{colName} = main.{colName}")));
            sbUpdate.Append($" SET temp.{identityColumnName} = main.{identityColumnName};");

            await ExecuteNonQuery(context, sbUpdate.ToString()).ConfigureAwait(false);
            
            var sbSelect = new StringBuilder();
            sbSelect.Append($"SELECT temp.{identityColumnName}, ");
            sbSelect.Append(string.Join(", ", lookupColumnNames));
            sbSelect.Append($" FROM {tempTableName} AS temp;");

            var results = await ExecuteReader(context, sbSelect.ToString()).ConfigureAwait(false);
            
            await ExecuteNonQuery(context, $"DROP TEMPORARY TABLE {tempTableName};").ConfigureAwait(false);

            return results;
        }

        public static string Insert<T>(
            EntityProperties entityProperties,
            IEnumerable<T> entities,
            bool onDuplicateKeyUpdate = false
            ) where T : class
        {
            var propertyList = entityProperties.Properties.ToArray();
            var entityList = entities.ToArray();
            var colCount = propertyList.Length;
            var rowCount = entityList.Length;
            var columnNames = propertyList.Select(p => $"`{p.GetColumnName()}`").ToArray();

            var sb = new StringBuilder();
            sb.Append($"INSERT INTO `{entityProperties.TableName}` (");
            sb.Append(string.Join(", ", columnNames));
            sb.Append(") VALUES ");
            sb.Append(GetPlaceholders(rowCount, colCount));

            if (onDuplicateKeyUpdate)
            {
                sb.Append(" ON DUPLICATE KEY UPDATE ");
                sb.Append(string.Join(", ", columnNames.Select(name => $"{name} = VALUES({name})"))); }
            sb.Append(';');
            
            return sb.ToString();
        }
        
        public static string Upsert<T>(
            EntityProperties entityProperties,
            IEnumerable<T> entities
            ) where T : class
        {
            var entityList = entities.ToArray();
            var rowCount = entityList.Length;
            var propertyList = entityProperties.Properties.ToArray();
            var columnNames = propertyList.Select(p => $"`{p.GetColumnName()}`").ToArray();
            var colCount = propertyList.Length;
            
            var sb = new StringBuilder();
            sb.Append($"INSERT INTO `{entityProperties.TableName}` (");
            sb.Append(string.Join(", ", columnNames));
            sb.Append(") VALUES ");
            sb.Append(GetPlaceholders(rowCount, colCount));
            sb.Append(" ON DUPLICATE KEY UPDATE ");
            sb.Append(string.Join(", ", columnNames.Select(name => $"{name} = VALUES({name})")));
            sb.Append(';');
            
            return sb.ToString();
        }

        public static Task Update<T>(
            DbContext context,
            EntityProperties entityProperties,
            IEnumerable<T> entities
            ) where T : class
        {
            // TODO: Implement Bulk Update logic
            throw new NotImplementedException("Bulk Update SQL statement generation is not implemented yet.");
        }
        
        private static string GetPlaceholders(int rowCount, int colCount)
        {
            var sb = new StringBuilder();
            var currentParamIndex = 0;
            for (var i = 0; i < rowCount; i++)
            {
                if (i > 0)
                    sb.Append(", ");
                sb.Append('(');
                for (var j = 0; j < colCount; j++)
                {
                    if (j > 0)
                        sb.Append(", ");
                    sb.Append('@').Append(currentParamIndex++);
                }
                sb.Append(')');
            }
            return sb.ToString();
        }

        #endregion

        #region Execution Helpers

        private static async Task ExecuteNonQuery(
            DbContext context,
            string sql
            )
        {
            if (context.Database.GetDbConnection() is not MySqlConnection connection)
                throw new InvalidOperationException(
                    "Database connection is not a MySqlConnection or is not available."
                );

            await using var cmd = new MySqlCommand(sql, connection);
            await cmd.ExecuteNonQueryAsync().ConfigureAwait(false);
        }
        
        private static async Task ExecuteNonQueryWithParameters<T>(
            DbContext context,
            string sql,
            IEnumerable<IProperty> properties,
            IEnumerable<T> entities
            ) where T : class
        {
            if (context.Database.GetDbConnection() is not MySqlConnection connection)
                throw new InvalidOperationException(
                    "Database connection is not a MySqlConnection or is not available."
                );

            await using var cmd = new MySqlCommand(sql, connection);
            
            var parameters = entities
                .SelectMany(entity =>
                    properties.Select(property =>
                        property.PropertyInfo?.GetValue(entity) ?? DBNull.Value
                    )
                )
                .ToArray();

            cmd.Parameters.AddRange(
                parameters
                    .Select((p, i) => new MySqlParameter($"@{i}", p))
                    .ToArray()
            );
            
            await cmd.ExecuteNonQueryAsync().ConfigureAwait(false);
        }
        
        private static async Task<IEnumerable<object[]>> ExecuteReader(
            DbContext context,
            string sql
            )
        {
            // var results = new List<object[]>();
            //
            // var results = await context.Database.ExecuteSqlRawAsync(sql).ConfigureAwait(false);
            // while (await reader.ReadAsync().ConfigureAwait(false))
            // {
            //     var rowValues = new object[reader.FieldCount];
            //     reader.GetValues(rowValues);
            //     results.Add(rowValues);
            // }

            return [];
        }
        
        private static async Task<IEnumerable<object[]>> ExecuteReaderWithParameters<
            T
        >(
            DbContext context,
            string sql,
            IEnumerable<IProperty> properties,
            IEnumerable<T> entities
            ) where T : class
        {
            if (context.Database.GetDbConnection() is not MySqlConnection connection)
                throw new InvalidOperationException(
                    "Database connection is not a MySqlConnection or is not available."
                );

            await using var cmd = new MySqlCommand(sql, connection);
            
            var parameters = entities
                .SelectMany(entity =>
                    properties.Select(property =>
                        property.PropertyInfo?.GetValue(entity) ?? DBNull.Value
                    )
                )
                .ToArray();

            cmd.Parameters.AddRange(
                parameters
                    .Select((p, i) => new MySqlParameter($"@{i}", p))
                    .ToArray()
            );
            
            var results = new List<object[]>();

            await using var reader = await cmd.ExecuteReaderAsync().ConfigureAwait(false);
            while (await reader.ReadAsync().ConfigureAwait(false))
            {
                var rowValues = new object[reader.FieldCount];
                reader.GetValues(rowValues);
                results.Add(rowValues);
            }

            return results;
        }

        #endregion
    }
}