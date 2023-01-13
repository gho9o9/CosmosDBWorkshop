using System;
using System.Collections.Generic;
using System.Linq;
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

        // 一本釣りクエリ
        ItemResponse<Food> candyResponse = await container.ReadItemAsync<Food>("19130", new PartitionKey("Sweets"));
        Food candy = candyResponse.Resource;
        Console.Out.WriteLine($"Read {candy.Description}");

        // 単一の Azure Cosmos DB パーティションに対してクエリを実行する
        string sqlA = "SELECT f.description, f.manufacturerName, f.servings FROM foods f WHERE f.foodGroup = 'Sweets' and IS_DEFINED(f.description) and IS_DEFINED(f.manufacturerName) and IS_DEFINED(f.servings)";
        FeedIterator<Food> queryA = container.GetItemQueryIterator<Food>(new QueryDefinition(sqlA), requestOptions: new QueryRequestOptions { MaxConcurrency = 1 });
        foreach (Food food in await queryA.ReadNextAsync())
        {
            await Console.Out.WriteLineAsync($"{food.Description} by {food.ManufacturerName}");
            foreach (Serving serving in food.Servings)
            {
                await Console.Out.WriteLineAsync($"\t{serving.Amount} {serving.Description}");
            }
            await Console.Out.WriteLineAsync();
        }

        // 複数の Azure Cosmos DB パーティションに対してクエリを実行する
        string sqlB = @"SELECT f.id, f.description, f.manufacturerName, f.servings FROM foods f WHERE IS_DEFINED(f.manufacturerName)";
        //MaxConcurrency が 0 に設定されている場合、コンテナーのパーティションへのネットワーク接続は 1 つです。
        //MaxItemCount は、クエリの待機時間とクライアント側のメモリ使用率をトレードします。
        //このオプションを省略するか、-1 に設定すると、SDK は並列クエリの実行中にバッファリングされる項目の数を管理します。
        //MaxItemCountをアイテムに制限しています。これにより、クエリに一致するアイテムが 100 個を超えると、ページングが発生します。
        FeedIterator<Food> queryB = container.GetItemQueryIterator<Food>(sqlB, requestOptions: new QueryRequestOptions { MaxConcurrency = 5, MaxItemCount = 100 });
        int pageCount = 0;
        while (queryB.HasMoreResults)
        {
            Console.Out.WriteLine($"---Page #{++pageCount:0000}---");
            foreach (var food in await queryB.ReadNextAsync())
            {
                Console.Out.WriteLine($"\t[{food.Id}]\t{food.Description,-20}\t{food.ManufacturerName,-40}");
            }
        }
    }
}
