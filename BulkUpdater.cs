using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata;

namespace Seiori.MySql;

public abstract class BulkUpdater
{
    public static async Task UpdateAsync<T>(this DbContext context, )
    {
        var entityList = entities.ToList();
        if (entityList.Count is 0) return;
        

    }
    
    public static string GetBulkUpdateSql<T>(string tableName, List<IProperty> keyProperties, List<IProperty> nonKeyProperties, List<IProperty> allProperties, IEnumerable<T> entities)
    {
        var entityList = entities.ToList();
        var setClause = string.Join(", ", nonKeyProperties.Select(p => $"{p.GetColumnName()} = VALUES({p.GetColumnName()})"));
        var whereClause = BuildWhereClause(keyProperties, entityList);
        return $"UPDATE {tableName} SET {setClause} WHERE {whereClause};";
    }
}