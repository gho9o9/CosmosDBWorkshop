using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Azure.Cosmos;

public class Program
{
    private static readonly string _endpointUri = "https://cosmoslab69931.documents.azure.com:443/";
    private static readonly string _primaryKey = "jSBZj4NhPF1oSsBpiZAZ88TtpcAhFLfwOZ3mpAqzDmd3Tdy5dVifXLX7NnD0OhKnu7zzO5fCdpAkymnFnQ13yg==";

    private static readonly string _databaseId = "FinancialDatabase";
    private static readonly string _peopleContainerId = "PeopleCollection";
    private static readonly string _transactionContainerId = "TransactionCollection";

    private static CosmosClient _client = new CosmosClient(_endpointUri, _primaryKey);

    public static async Task Main(string[] args)
    {

        Database database = _client.GetDatabase(_databaseId);
        Container peopleContainer = database.GetContainer(_peopleContainerId);
        Container transactionContainer = database.GetContainer(_transactionContainerId);

        //await CreateMember(peopleContainer);
        //await CreateTransactions(transactionContainer);
        //await QueryTransactions(transactionContainer);
        //await QueryTransactions2(transactionContainer);
        //await QueryMember(peopleContainer);
        //await ReadMember(peopleContainer);
        //await EstimateThroughput(peopleContainer);
        await UpdateThroughput(peopleContainer);
    }

    private static async Task<double> CreateMember(Container peopleContainer)
    {
        //object member = new Member { accountHolder = new Bogus.Person() };
        object member = new Member
        {
            accountHolder = new Bogus.Person(),
            relatives = new Family
            {
                spouse = new Bogus.Person(),
                children = Enumerable.Range(0, 4).Select(r => new Bogus.Person())
            }
        };

        ItemResponse<object> response = await peopleContainer.CreateItemAsync(member);
        await Console.Out.WriteLineAsync($"{response.RequestCharge} RU/s");
        return response.RequestCharge;
    }

    private static async Task CreateTransactions(Container transactionContainer)
    {
        var transactions = new Bogus.Faker<Transaction>()
            .RuleFor(t => t.id, (fake) => Guid.NewGuid().ToString())
            .RuleFor(t => t.amount, (fake) => Math.Round(fake.Random.Double(5, 500), 2))
            .RuleFor(t => t.processed, (fake) => fake.Random.Bool(0.6f))
            .RuleFor(t => t.paidBy, (fake) => $"{fake.Name.FirstName().ToLower()}.{fake.Name.LastName().ToLower()}")
            .RuleFor(t => t.costCenter, (fake) => fake.Commerce.Department(1).ToLower())
            .GenerateLazy(5000);

        /*
        foreach (var transaction in transactions)
        {
            ItemResponse<Transaction> result = await transactionContainer.CreateItemAsync(transaction);
            await Console.Out.WriteLineAsync($"Item Created\t{result.Resource.id}");
        }
        */
        List<Task<ItemResponse<Transaction>>> tasks = new List<Task<ItemResponse<Transaction>>>();
        foreach (var transaction in transactions)
        {
            Task<ItemResponse<Transaction>> resultTask = transactionContainer.CreateItemAsync(transaction);
            tasks.Add(resultTask);
        }
        Task.WaitAll(tasks.ToArray());
        foreach (var task in tasks)
        {
            await Console.Out.WriteLineAsync($"Item Created\t{task.Result.Resource.id}");
        }
    }

    private static async Task QueryTransactions(Container transactionContainer)
    {
        //string sql = "SELECT TOP 1000 * FROM c WHERE c.processed = true ORDER BY c.amount DESC";
        //string sql = "SELECT * FROM c WHERE c.processed = true";
        //string sql = "SELECT * FROM c";
        string sql = "SELECT c.id FROM c";
        FeedIterator<Transaction> query = transactionContainer.GetItemQueryIterator<Transaction>(sql);
        var result = await query.ReadNextAsync();
        await Console.Out.WriteLineAsync($"Request Charge: {result.RequestCharge} RU/s");
    }

    private static async Task QueryTransactions2(Container transactionContainer)
    {
        //int maxItemCount = 100;
        //int maxItemCount = 500;
        int maxItemCount = 1000;
        //int maxDegreeOfParallelism = 1;
        //int maxDegreeOfParallelism = 5;
        int maxDegreeOfParallelism = -1;
        //int maxBufferedItemCount = 0;
        //int maxBufferedItemCount = -1;
        int maxBufferedItemCount = 50000;

        QueryRequestOptions options = new QueryRequestOptions
        {
            MaxItemCount = maxItemCount,
            MaxBufferedItemCount = maxBufferedItemCount,
            MaxConcurrency = maxDegreeOfParallelism
        };

        await Console.Out.WriteLineAsync($"MaxItemCount:\t{maxItemCount}");
        await Console.Out.WriteLineAsync($"MaxDegreeOfParallelism:\t{maxDegreeOfParallelism}");
        await Console.Out.WriteLineAsync($"MaxBufferedItemCount:\t{maxBufferedItemCount}");

        string sql = "SELECT * FROM c WHERE c.processed = true ORDER BY c.amount DESC";

        Stopwatch timer = Stopwatch.StartNew();

        FeedIterator<Transaction> query = transactionContainer.GetItemQueryIterator<Transaction>(sql, requestOptions: options);
        while (query.HasMoreResults)
        {
            var result = await query.ReadNextAsync();
        }
        timer.Stop();
        await Console.Out.WriteLineAsync($"Elapsed Time:\t{timer.Elapsed.TotalSeconds}");

        /*
            PS C:\Users\tooota\OneDrive - Microsoft\Git\CosmosDBWorkshop\Demo\Lab09>  dotnet run
            MaxItemCount:   100
            MaxDegreeOfParallelism: 1
            MaxBufferedItemCount:   0
            Elapsed Time:   11.7034495
            PS C:\Users\tooota\OneDrive - Microsoft\Git\CosmosDBWorkshop\Demo\Lab09>  dotnet run
            MaxItemCount:   100
            MaxDegreeOfParallelism: 5
            MaxBufferedItemCount:   0
            Elapsed Time:   11.7500637
            PS C:\Users\tooota\OneDrive - Microsoft\Git\CosmosDBWorkshop\Demo\Lab09>  dotnet run
            MaxItemCount:   100
            MaxDegreeOfParallelism: 5
            MaxBufferedItemCount:   0
            Elapsed Time:   10.6810932
            PS C:\Users\tooota\OneDrive - Microsoft\Git\CosmosDBWorkshop\Demo\Lab09>  dotnet run
            MaxItemCount:   100
            MaxDegreeOfParallelism: 5
            MaxBufferedItemCount:   -1
            Elapsed Time:   7.5600015
            PS C:\Users\tooota\OneDrive - Microsoft\Git\CosmosDBWorkshop\Demo\Lab09>  dotnet run
            MaxItemCount:   100
            MaxDegreeOfParallelism: -1
            MaxBufferedItemCount:   -1
            Elapsed Time:   7.2692033
            PS C:\Users\tooota\OneDrive - Microsoft\Git\CosmosDBWorkshop\Demo\Lab09>  dotnet run
            MaxItemCount:   500
            MaxDegreeOfParallelism: -1
            MaxBufferedItemCount:   -1
            Elapsed Time:   5.5418275
            PS C:\Users\tooota\OneDrive - Microsoft\Git\CosmosDBWorkshop\Demo\Lab09>  dotnet run
            MaxItemCount:   1000
            MaxDegreeOfParallelism: -1
            MaxBufferedItemCount:   -1
            Elapsed Time:   5.209951
            PS C:\Users\tooota\OneDrive - Microsoft\Git\CosmosDBWorkshop\Demo\Lab09>  dotnet run
            MaxItemCount:   1000
            MaxDegreeOfParallelism: -1
            MaxBufferedItemCount:   50000
            Elapsed Time:   5.4150037
        */
    }

    private static async Task QueryMember(Container peopleContainer)
    {
        string sql = "SELECT TOP 1 * FROM c WHERE c.id = '360bb839-e8df-4a07-9cd4-c0b82365c103'";
        FeedIterator<object> query = peopleContainer.GetItemQueryIterator<object>(sql);
        FeedResponse<object> response = await query.ReadNextAsync();

        await Console.Out.WriteLineAsync($"{response.Resource.First()}");
        await Console.Out.WriteLineAsync($"{response.RequestCharge} RU/s");
    }

    private static async Task<double> ReadMember(Container peopleContainer)
    {
        ItemResponse<object> response = await peopleContainer.ReadItemAsync<object>("360bb839-e8df-4a07-9cd4-c0b82365c103", new PartitionKey("Mosciski"));
        await Console.Out.WriteLineAsync($"{response.RequestCharge} RU/s");
        return response.RequestCharge;
    }

    private static async Task EstimateThroughput(Container peopleContainer)
    {
        int expectedWritesPerSec = 200;
        int expectedReadsPerSec = 800;

        double writeCost = await CreateMember(peopleContainer);
        double readCost = await ReadMember(peopleContainer);

        await Console.Out.WriteLineAsync($"Estimated load: {writeCost * expectedWritesPerSec + readCost * expectedReadsPerSec} RU/s");
    }

    private static async Task UpdateThroughput(Container peopleContainer)
    {
        int? throughput = await peopleContainer.ReadThroughputAsync();
        await Console.Out.WriteLineAsync($"Current Throughput {throughput} RU/s");

        ThroughputResponse throughputResponse = await peopleContainer.ReadThroughputAsync(new RequestOptions());
        int? minThroughput = throughputResponse.MinThroughput;
        await Console.Out.WriteLineAsync($"Minimum Throughput {minThroughput} RU/s");

        await peopleContainer.ReplaceThroughputAsync(1000);
        throughput = await peopleContainer.ReadThroughputAsync();
        await Console.Out.WriteLineAsync($"New Throughput {throughput} RU/s");
    }

}
