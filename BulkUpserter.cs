namespace Seiori.MySql;

public class BulkUpserter
{
    public static async Task UpserterAsync<T>(this DbContext context)
    {
        var entityList = entities.ToList();
        if (entityList.Count is 0) return;
        

    }
}