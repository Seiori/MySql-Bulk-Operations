using System.Text;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata;
using MySqlConnector;
using Seiori.MySql.Classes;

namespace Seiori.MySql.Helpers;

public static class BuildSqlStatement
{
    public static object[] BuildParameters<T>(IEnumerable<T> entities, IEnumerable<IProperty> properties)
    {
        return entities.SelectMany(entity =>
            properties.Select(property => property.PropertyInfo?.GetValue(entity) ?? DBNull.Value)
        ).ToArray();
    }
    
    public static MySqlParameter[] BuildLookupParameters<T>(IEnumerable<T> entities, IEnumerable<IProperty> properties)
    {
        var propertyList = properties.ToArray();
        var parameters = new List<MySqlParameter>();
        foreach (var entity in entities)
        {
            foreach (var prop in propertyList)
            {
                var value = prop.PropertyInfo?.GetValue(entity) ?? DBNull.Value;
                parameters.Add(new MySqlParameter($"@p{parameters.Count}", value));
            }
        }
        return parameters.ToArray();
    }
    
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
    
    public static string BuildInsertSql(string tableName, int rowCount, IEnumerable<IProperty> properties)
    {
        var propertyList = properties.ToArray();
        var colCount = propertyList.Length;
        var sb = new StringBuilder();
        sb.Append($"INSERT INTO {tableName} (");
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
    
    public static string BuildUpsertSql(string tableName, int rowCount, IEnumerable<IProperty> props)
    {
        var propertyList = props.ToArray();
        var sb = new StringBuilder();
        sb.Append($"INSERT INTO {tableName} (");
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

    public static string BuildUpdateSql<T>(IEnumerable<T> entities, EntityProperties entityProps)
    {
        var entityList = entities.ToArray();

        return "";
    }
}