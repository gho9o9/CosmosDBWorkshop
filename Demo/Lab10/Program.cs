using System;
using System.Threading.Tasks;
using Microsoft.Azure.Cosmos;

public class Program
{
    private static readonly string _endpointUri = "https://cosmoslab69931.documents.azure.com:443/";
    private static readonly string _primaryKey = "jSBZj4NhPF1oSsBpiZAZ88TtpcAhFLfwOZ3mpAqzDmd3Tdy5dVifXLX7NnD0OhKnu7zzO5fCdpAkymnFnQ13yg==";
    private static readonly string _databaseId = "NutritionDatabase";
    private static readonly string _containerId = "FoodCollection";
    private static CosmosClient _client = new CosmosClient(_endpointUri, _primaryKey);

    public static async Task Main(string[] args)
    {
        Database database = _client.GetDatabase(_databaseId);
        Container container = database.GetContainer(_containerId);

        ItemResponse<Food> response = await container.ReadItemAsync<Food>("21083", new PartitionKey("Fast Foods"));
        //await Console.Out.WriteLineAsync($"ETag: {response.ETag}");
        await Console.Out.WriteLineAsync($"Existing ETag:\t{response.ETag}");

        ItemRequestOptions requestOptions = new ItemRequestOptions { IfMatchEtag = response.ETag };
        response.Resource.tags.Add(new Tag { name = "Demo" });

        response = await container.UpsertItemAsync(response.Resource, requestOptions: requestOptions);
        await Console.Out.WriteLineAsync($"New ETag:\t{response.ETag}");

        response.Resource.tags.Add(new Tag { name = "Failure" });
        try
        {
            response = await container.UpsertItemAsync(response.Resource, requestOptions: requestOptions);
        }
        catch (Exception ex)
        {
            await Console.Out.WriteLineAsync($"Update error:\t{ex.Message}");
        }

        response.Resource.tags.Add(new Tag { name = "Success" });
        try
        {
            response = await container.UpsertItemAsync(response.Resource, requestOptions: null);
            await Console.Out.WriteLineAsync($"New ETag:\t{response.ETag}");
        }
        catch (Exception ex)
        {
            await Console.Out.WriteLineAsync($"Update error:\t{ex.Message}");
        }
    }
}
