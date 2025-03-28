using System.Text;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata;
using MySqlConnector;
using Seiori.MySql.Classes;

namespace Seiori.MySql.Helpers;

public class BuildSqlStatement
{
    public static object[] BuildParameters<T>(EntityProperties<T> entityProps)
    {
        var entities = entityProps.Entities.ToArray();
        return entities.SelectMany(entity =>
            entityProps.Properties.Select(property => property.PropertyInfo?.GetValue(entity) ?? DBNull.Value)
        ).ToArray();
    }
    
    public static MySqlParameter[] BuildLookupParameters<T>(EntityProperties<T> entityProps, IEnumerable<IProperty> lookupProps)
    {
        var entities = entityProps.Entities.ToArray();
        var lookupProperties = lookupProps.ToArray();
        
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
    
    public static string BuildSelectSql<T>(EntityProperties<T> entityProps)
    {
        var lookupProperties = entityProps.PrimaryKey?.Properties;
        if (lookupProperties == null)
        {
            var firstAlternateKey = entityProps.AlternateKeys?.FirstOrDefault(ak => ak.Properties.Any());
            lookupProperties = firstAlternateKey != null ? firstAlternateKey.Properties : entityProps.Properties.ToArray();
        }

        var sb = new StringBuilder();
        sb.Append("SELECT ");
        sb.Append(string.Join(", ", entityProps.Properties.Select(p => $"`{p.GetColumnName()}`")));
        sb.Append(" FROM ");
        sb.Append(entityProps.TableName);
        sb.Append(" WHERE ");
        var paramIndex = 0;
        var conditionList = entityProps.Entities.Select(_ => lookupProperties.Select(p => $"{p.GetColumnName()} = @p{paramIndex++}")).Select(conditions => $"({string.Join(" AND ", conditions)})").ToList();
        sb.Append(string.Join(" OR ", conditionList));
        sb.Append(';');
        return sb.ToString();
    }
    
    public static string BuildInsertSql<T>(string tableName, IEnumerable<T> entities, ICollection<IProperty> allProperties)
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
    
    public static string BuildUpsertSql<T>(string tableName, IEnumerable<T> entities, ICollection<IProperty> keyProperties, ICollection<IProperty> allProperties, )
    {
        var entityList = entities.ToArray();
        var rowCount = entityList.Length;
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
        sb.Append(" ON DUPLICATE KEY UPDATE ");
        var updateColumns = allProperties.Where(p => !keyProperties.Contains(p)).Select(p => $"{p.GetColumnName()} = VALUES({p.GetColumnName()})");
        sb.Append(string.Join(", ", updateColumns));
        return sb.ToString();
    }
    
    public static string BuildWhereClause<T>(List<IProperty> keyProperties, List<T> entities)
    {
        var clauses = new string[entities.Count];
        for (var i = 0; i < entities.Count; i++)
        {
            var conditions = keyProperties.Select(p => $"{p.GetColumnName()} = @{i * keyProperties.Count + keyProperties.IndexOf(p)}");
            clauses[i] = $"({string.Join(" AND ", conditions)})";
        }
        return string.Join(" OR ", clauses);
    }
}