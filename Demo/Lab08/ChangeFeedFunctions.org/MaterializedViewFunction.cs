using System;
using System.Collections.Generic;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;

namespace ChangeFeedFunctions
{
    public class MaterializedViewFunction
    {
        private readonly ILogger _logger;

        public MaterializedViewFunction(ILoggerFactory loggerFactory)
        {
            _logger = loggerFactory.CreateLogger<MaterializedViewFunction>();
        }

        [Function("MaterializedViewFunction")]
        public void Run([CosmosDBTrigger(
            databaseName: "databaseName",
            collectionName: "collectionName",
            ConnectionStringSetting = "",
            LeaseCollectionName = "leases")] IReadOnlyList<MyDocument> input)
        {
            if (input != null && input.Count > 0)
            {
                _logger.LogInformation("Documents modified: " + input.Count);
                _logger.LogInformation("First document Id: " + input[0].Id);
            }
        }
    }

    public class MyDocument
    {
        public string Id { get; set; }

        public string Text { get; set; }

        public int Number { get; set; }

        public bool Boolean { get; set; }
    }
}
