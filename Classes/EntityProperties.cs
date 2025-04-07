using Microsoft.EntityFrameworkCore.Metadata;

namespace Seiori.MySql.Classes
{
    public class EntityProperties
    {
        public required string TableName { get; init; }
        public required IEnumerable<IProperty> Properties { get; init; }
        public IProperty? IdentityProperty { get; init; }
        public required IEnumerable<IProperty> KeyProperties { get; init; }
        public IEnumerable<INavigation>? NavigationProperties { get; init; }
    }
}