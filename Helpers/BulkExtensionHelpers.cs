using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata;
using Seiori.MySql.Classes;

namespace Seiori.MySql.Helpers;

public static class BulkExtensionHelpers
{
    public static EntityProperties GetEntityProperties(IEntityType entityType)
    {
        var tableName = entityType.GetTableName();
        var keyProperties = entityType.GetProperties().Where(p => p.IsKey());
        var properties = entityType.GetProperties().Where(p => p.ValueGenerated == ValueGenerated.Never);
        var navigationProperties = entityType.GetNavigations().ToArray();
        
        return new EntityProperties
        {
            TableName = tableName ?? throw new InvalidOperationException("Table name could not be determined."),
            KeyProperties = keyProperties,
            Properties = properties,
            NavigationProperties = navigationProperties
        };
    }
}