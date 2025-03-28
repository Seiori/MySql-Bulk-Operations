using Microsoft.EntityFrameworkCore.Metadata;

namespace Seiori.MySql.Classes;

public class EntityProperties<T>
{
    public required string TableName { get; set; }
    public IKey? PrimaryKey { get; set; }
    public IEnumerable<IKey>? AlternateKeys { get; set; }
    public required IEnumerable<IProperty> Properties { get; set; }
    public IEnumerable<INavigation>? NavigationProperties { get; set; }
    public required IEnumerable<T> Entities { get; set; }
}