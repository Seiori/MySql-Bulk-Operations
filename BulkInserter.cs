using System.Text;
using Microsoft.EntityFrameworkCore;
using MySqlConnector;

namespace Seiori.MySql;

public static class BulkInserter
{
    public static async Task InsertAsync<T>(this DbContext context)
    {
        var entityList = entities.ToList();
        if (entityList.Count is 0) return;
        

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
}