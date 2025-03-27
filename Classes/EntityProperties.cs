using Microsoft.EntityFrameworkCore.Metadata;

namespace Seiori.MySql.Classes;

public class EntityProperties
{
    public string? TableName { get; set; }
    public IProperty? PrimaryKey { get; set; }
    public ICollection<IProperty>? Properties { get; set; }
}