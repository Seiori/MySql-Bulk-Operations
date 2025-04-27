using Microsoft.EntityFrameworkCore.Metadata;

namespace Seiori.MySql.Helpers;

public class EntityKeyEqualityComparer<T> : IEqualityComparer<T> where T : class
{
    private readonly IReadOnlyList<IProperty> _keyProperties;

    public EntityKeyEqualityComparer(IEnumerable<IProperty> keyProperties)
    {
        _keyProperties = keyProperties.ToArray() ?? throw new ArgumentNullException(nameof(keyProperties));
        if (_keyProperties.Count == 0)
        {
            throw new ArgumentException("At least one key property is required.", nameof(keyProperties));
        }
    }

    public EntityKeyEqualityComparer(IReadOnlyList<IProperty> keyProperties)
    {
        _keyProperties = keyProperties;
    }

    public bool Equals(T? x, T? y)
    {
        if (ReferenceEquals(x, y)) return true;
        if (x is null || y is null) return false;

        return !(from prop in _keyProperties let xValue = prop.PropertyInfo?.GetValue(x) let yValue = prop.PropertyInfo?.GetValue(y) where !object.Equals(xValue, yValue) select xValue).Any();
    }

    public int GetHashCode(T? obj)
    {
        return obj is null ? 0 : _keyProperties.Select(prop => prop.PropertyInfo?.GetValue(obj)).Aggregate(17, (current, value) => current * 31 + (value?.GetHashCode() ?? 0));
    }
}