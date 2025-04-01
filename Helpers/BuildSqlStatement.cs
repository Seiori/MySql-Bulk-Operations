using System.Text;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata;
using Seiori.MySql.Classes;

namespace Seiori.MySql.Helpers
{
    /// <summary>
    /// Provides methods to build SQL statements for bulk operations,
    /// including SELECT for identity values, INSERT, UPSERT, and UPDATE commands.
    /// </summary>
    public static class BuildSqlStatement
    {
        /// <summary>
        /// Builds a SELECT SQL statement that retrieves the identity column and associated lookup columns
        /// for a specified table and number of rows.
        /// </summary>
        /// <param name="tableName">The name of the table from which to select identity and lookup columns.</param>
        /// <param name="rowCount">The number of rows (entities) for which the query will generate conditions.</param>
        /// <param name="identityProp">The property representing the auto-generated identity column.</param>
        /// <param name="lookupProps">A collection of properties that serve as lookup columns to identify the rows.</param>
        /// <returns>
        /// A SQL query string which selects the identity column and the lookup columns from the specified table,
        /// with conditions that allow retrieval of identity values for the given rows.
        /// </returns>
        public static string BuildSelectIdentitySql(string tableName, int rowCount, IProperty identityProp, IEnumerable<IProperty> lookupProps)
        {
            var lookupProperties = lookupProps.ToArray();
            var selectColumns = new List<string>();
            selectColumns.AddRange(identityProp.GetColumnName());
            selectColumns.AddRange(lookupProperties.Select(lp => lp.GetColumnName()));

            var sb = new StringBuilder();
            sb.Append("SELECT ");
            sb.Append(string.Join(", ", selectColumns));
            sb.Append($" FROM {tableName} WHERE ");
        
            var conditionsList = new string[rowCount];
            var lookupColCount = lookupProperties.Length;
            for (var i = 0; i < rowCount; i++)
            {
                var conditions = new string[lookupColCount];
                for (var j = 0; j < lookupColCount; j++)
                {
                    conditions[j] = $"{lookupProperties[j].GetColumnName()} = @{i * lookupColCount + j}";
                }
                conditionsList[i] = $"({string.Join(" AND ", conditions)})";
            }
            sb.Append(string.Join(" OR ", conditionsList));
            return sb.ToString();
        }
    
        /// <summary>
        /// Builds an INSERT SQL statement for bulk inserting multiple rows into a table.
        /// </summary>
        /// <param name="entityProps">
        /// The metadata for the entity, which includes the table name and the collection of properties
        /// used for generating the list of columns to insert.
        /// </param>
        /// <param name="rowCount">The number of rows (entities) to be inserted.</param>
        /// <returns>
        /// A SQL INSERT statement that inserts the specified rows into the table using parameterized values.
        /// </returns>
        public static string BuildInsertSql(EntityProperties entityProps, int rowCount)
        {
            var propertyList = entityProps.Properties.ToArray();
            var colCount = propertyList.Length;
            var sb = new StringBuilder();
            sb.Append($"INSERT INTO {entityProps.TableName} (");
            sb.Append(string.Join(", ", propertyList.Select(p => p.GetColumnName())));
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
    
        /// <summary>
        /// Builds an UPSERT SQL statement for bulk operations that insert or update rows.
        /// 
        /// The statement uses an INSERT command with an ON DUPLICATE KEY UPDATE clause so that
        /// any duplicate key conflicts will trigger an update of the existing row.
        /// </summary>
        /// <param name="entityProps">
        /// The metadata for the entity, which must include key properties and the list of properties for generating
        /// the INSERT portion of the statement. An exception is thrown if no keys are defined.
        /// </param>
        /// <param name="rowCount">The number of rows (entities) to be processed.</param>
        /// <returns>
        /// A SQL statement that attempts to insert multiple rows, and on key conflicts, updates the existing row values.
        /// </returns>
        public static string BuildUpsertSql(EntityProperties entityProps, int rowCount)
        {
            if (entityProps.KeyProperties is null || entityProps.KeyProperties.Any() is false)
                throw new InvalidOperationException($"The table {entityProps.TableName} does not have any keys defined.");
        
            var propertyList = entityProps.Properties.ToArray();
            var sb = new StringBuilder();
            sb.Append($"INSERT INTO {entityProps.TableName} (");
            sb.Append(string.Join(", ", propertyList.Select(p => p.GetColumnName())));
            sb.Append(") VALUES ");
            var colCount = propertyList.Length;
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
            sb.Append(" ON DUPLICATE KEY UPDATE ");
            sb.Append(string.Join(", ", propertyList.Select(p => $"{p.GetColumnName()} = VALUES({p.GetColumnName()})")));
            return sb.ToString();
        }

        /// <summary>
        /// Builds an UPDATE SQL statement for bulk updating one or more rows in a table.
        /// 
        /// The statement sets new values for the columns defined in the entity properties
        /// and includes a WHERE clause that combines conditions based on the key properties
        /// to uniquely identify each row. An exception is thrown if no keys are defined.
        /// </summary>
        /// <param name="entityProps">
        /// The metadata for the entity, including the table name, the properties to update,
        /// and the key properties used for constructing the WHERE clause.
        /// </param>
        /// <param name="rowCount">The number of rows (entities) to be updated.</param>
        /// <returns>
        /// A SQL UPDATE statement that updates the specified columns with new values defined by parameters,
        /// and applies conditions based on key properties to target specific rows.
        /// </returns>
        public static string BuildUpdateSql(EntityProperties entityProps, int rowCount)
        {
            if (entityProps.KeyProperties is null || entityProps.KeyProperties.Any() is false)
                throw new InvalidOperationException($"The table {entityProps.TableName} does not have any keys defined.");
        
            var propertyList = entityProps.Properties.ToArray();
            var sb = new StringBuilder();
            sb.Append($"UPDATE {entityProps.TableName} SET ");
            sb.Append(string.Join(", ", propertyList.Select(p => $"{p.GetColumnName()} = @{p.Name}")));
            sb.Append(" WHERE ");
        
            var keyProperties = entityProps.KeyProperties.ToArray();
            var conditions = new string[rowCount];
            for (var i = 0; i < rowCount; i++)
            {
                var conditionList = new string[keyProperties.Length];
                for (var j = 0; j < keyProperties.Length; j++)
                {
                    conditionList[j] = $"{keyProperties[j].GetColumnName()} = @{i * keyProperties.Length + j}";
                }
                conditions[i] = $"({string.Join(" AND ", conditionList)})";
            }
            sb.Append(string.Join(" OR ", conditions));
            return sb.ToString();
        }
    }
}