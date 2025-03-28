using System.Text;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata;
using MySqlConnector;
using Seiori.MySql.Classes;

namespace Seiori.MySql;

public static class BulkExtensionHelpers
{
    public static EntityProperties GetEntityProperties(IEntityType entityType)
    {
        var tableName = entityType.GetTableName();
        var primaryKey = entityType.FindPrimaryKey();
        var alternateKeys = entityType.GetKeys().Where(k => !k.IsPrimaryKey()).ToArray();
        var properties = entityType.GetProperties().ToArray();
        var navigationProperties = entityType.GetNavigations().ToArray();
        
        return new EntityProperties
        {
            TableName = tableName,
            PrimaryKey = primaryKey,
            AlternateKeys = alternateKeys,
            Properties = properties,
            NavigationProperties = navigationProperties
        };
    }
    
    public static void UpdateForeignKeysForNavigation(object parent, INavigation navigation)
    {
        var foreignKey = navigation.ForeignKey;
        var dependentProperties = foreignKey.Properties;
        var principalProperties = foreignKey.PrincipalKey.Properties;
        var pkValues = principalProperties.Select(p => p.PropertyInfo!.GetValue(parent)).ToList();
        if (pkValues.Any(v => v is null))
            return;
        if (navigation.PropertyInfo.GetValue(parent) is not IEnumerable childCollection) return;
        foreach (var child in childCollection)
        {
            var i = 0;
            foreach (var depProp in dependentProperties)
            {
                depProp.PropertyInfo!.SetValue(child, pkValues[i]!);
                i++;
            }
        }
    }
    
        /// <summary>
    /// Processes child entities of the parent entities via navigation properties.
    /// If <see cref="BulkOptions.SetOutputIdentity"/> is true, each child's foreign keys are updated
    /// to match the parent's new key before recursively bulk inserting the children.
    /// </summary>
    public static async Task ProcessChildEntitiesAsync<T>(DbContext context, IEnumerable<T> parentEntities, BulkOptions options)
    {
        var entityType = context.Model.FindEntityType(typeof(T)) ?? throw new InvalidOperationException($"No entity type information found for {nameof(T)}.");

        foreach (var navigation in entityType.GetNavigations().Where(n => n.IsCollection))
        {
            if (options.SetOutputIdentity)
            {
                foreach (var parent in parentEntities)
                    UpdateForeignKeysForNavigation(parent, navigation);
            }

            var children = new List<object>();
            foreach (var parent in parentEntities)
            {
                if (navigation.PropertyInfo.GetValue(parent) is not IEnumerable childCollection) continue;
                children.AddRange(childCollection.Cast<object>());
            }

            if (children.Count <= 0) continue;
            var childType = navigation.PropertyInfo.PropertyType
                .GetInterfaces()
                .FirstOrDefault(i => i.IsGenericType && i.GetGenericTypeDefinition() == typeof(IEnumerable<>))
                ?.GetGenericArguments()[0] ?? navigation.TargetEntityType.ClrType;

            var genericMethod = BulkInsertMethodCache.GetOrAdd(childType, t =>
            {
                var method = typeof(DbContextExtensions).GetMethod(nameof(BulkInsertAsync));
                if (method is null)
                    throw new InvalidOperationException("BulkInsertAsync method not found.");
                return method.MakeGenericMethod(t);
            });

            var castMethod = typeof(Enumerable).GetMethod("Cast")!.MakeGenericMethod(childType);
            var typedChildren = castMethod.Invoke(null, [children]);
            var toListMethod = typeof(Enumerable).GetMethod("ToList")!.MakeGenericMethod(childType);
            var finalChildren = toListMethod.Invoke(null, [typedChildren]);

            var task = (Task)genericMethod.Invoke(null, [context, finalChildren, (Action<BulkOptions>)ChildOptions])!;
            await task.ConfigureAwait(false);
            continue;

            void ChildOptions(BulkOptions o)
            {
                o.BatchSize = options.BatchSize;
                o.SetOutputIdentity = options.SetOutputIdentity;
                o.IncludeChildren = options.IncludeChildren;
            }
        }
    }
}