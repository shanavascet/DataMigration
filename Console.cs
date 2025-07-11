using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Azure.Cosmos;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using System.Threading;

// Configuration model
public class CosmosConfig
{
    public string ConnectionString { get; set; }
    public string DatabaseName { get; set; }
    public string SourceContainerName { get; set; }
    public string DestinationContainerName { get; set; }
    public int BatchSize { get; set; } = 100;
    public int MaxConcurrency { get; set; } = 10;
}

// Source data model (example)
public class SourceRecord
{
    public string id { get; set; }
    public string Name { get; set; }
    public string Email { get; set; }
    public DateTime CreatedDate { get; set; }
    public string Status { get; set; }
    public Dictionary<string, object> AdditionalProperties { get; set; }
}

// Destination data model (example)
public class DestinationRecord
{
    public string id { get; set; }
    public string FullName { get; set; }
    public string EmailAddress { get; set; }
    public DateTime ProcessedDate { get; set; }
    public bool IsActive { get; set; }
    public string TransformationVersion { get; set; }
    public Dictionary<string, object> Metadata { get; set; }
}

// Migration progress tracking
public class MigrationProgress
{
    public string id { get; set; } = "migration_progress";
    public int TotalRecords { get; set; }
    public int ProcessedRecords { get; set; }
    public int FailedRecords { get; set; }
    public DateTime StartTime { get; set; }
    public DateTime? EndTime { get; set; }
    public string Status { get; set; }
    public List<string> FailedRecordIds { get; set; } = new List<string>();
}

public class CosmosMigrationService
{
    private readonly CosmosClient _cosmosClient;
    private readonly Container _sourceContainer;
    private readonly Container _destinationContainer;
    private readonly Container _progressContainer;
    private readonly ILogger<CosmosMigrationService> _logger;
    private readonly CosmosConfig _config;
    private readonly SemaphoreSlim _semaphore;

    public CosmosMigrationService(CosmosConfig config, ILogger<CosmosMigrationService> logger)
    {
        _config = config;
        _logger = logger;
        _cosmosClient = new CosmosClient(config.ConnectionString);
        
        var database = _cosmosClient.GetDatabase(config.DatabaseName);
        _sourceContainer = database.GetContainer(config.SourceContainerName);
        _destinationContainer = database.GetContainer(config.DestinationContainerName);
        _progressContainer = database.GetContainer("migration_progress"); // Create this container for tracking
        
        _semaphore = new SemaphoreSlim(config.MaxConcurrency, config.MaxConcurrency);
    }

    public async Task<bool> MigrateDataAsync(CancellationToken cancellationToken = default)
    {
        var progress = new MigrationProgress
        {
            StartTime = DateTime.UtcNow,
            Status = "Starting"
        };

        try
        {
            _logger.LogInformation("Starting data migration from {Source} to {Destination}", 
                _config.SourceContainerName, _config.DestinationContainerName);

            // Get total count
            progress.TotalRecords = await GetTotalRecordCountAsync();
            progress.Status = "In Progress";
            await SaveProgressAsync(progress);

            _logger.LogInformation("Total records to migrate: {Count}", progress.TotalRecords);

            // Process in batches
            await ProcessInBatchesAsync(progress, cancellationToken);

            progress.EndTime = DateTime.UtcNow;
            progress.Status = progress.FailedRecords > 0 ? "Completed with errors" : "Completed";
            await SaveProgressAsync(progress);

            _logger.LogInformation("Migration completed. Processed: {Processed}, Failed: {Failed}", 
                progress.ProcessedRecords, progress.FailedRecords);

            return progress.FailedRecords == 0;
        }
        catch (Exception ex)
        {
            progress.Status = "Failed";
            progress.EndTime = DateTime.UtcNow;
            await SaveProgressAsync(progress);
            
            _logger.LogError(ex, "Migration failed");
            throw;
        }
    }

    private async Task<int> GetTotalRecordCountAsync()
    {
        var query = "SELECT VALUE COUNT(1) FROM c";
        var iterator = _sourceContainer.GetItemQueryIterator<int>(query);
        var result = await iterator.ReadNextAsync();
        return result.FirstOrDefault();
    }

    private async Task ProcessInBatchesAsync(MigrationProgress progress, CancellationToken cancellationToken)
    {
        string continuationToken = null;
        
        do
        {
            var queryOptions = new QueryRequestOptions
            {
                MaxItemCount = _config.BatchSize
            };

            var query = "SELECT * FROM c";
            var iterator = _sourceContainer.GetItemQueryIterator<SourceRecord>(
                query, continuationToken, queryOptions);

            while (iterator.HasMoreResults && !cancellationToken.IsCancellationRequested)
            {
                var batch = await iterator.ReadNextAsync(cancellationToken);
                continuationToken = batch.ContinuationToken;

                // Process batch concurrently
                var tasks = batch.Select(record => ProcessRecordAsync(record, progress));
                await Task.WhenAll(tasks);

                // Update progress periodically
                if (progress.ProcessedRecords % 1000 == 0)
                {
                    await SaveProgressAsync(progress);
                    _logger.LogInformation("Progress: {Processed}/{Total} records processed", 
                        progress.ProcessedRecords, progress.TotalRecords);
                }
            }
        } while (continuationToken != null && !cancellationToken.IsCancellationRequested);
    }

    private async Task ProcessRecordAsync(SourceRecord sourceRecord, MigrationProgress progress)
    {
        await _semaphore.WaitAsync();
        
        try
        {
            // Transform the record
            var transformedRecord = TransformRecord(sourceRecord);
            
            // Save to destination container
            await _destinationContainer.UpsertItemAsync(transformedRecord, 
                new PartitionKey(transformedRecord.id));
            
            Interlocked.Increment(ref progress.ProcessedRecords);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to process record {Id}", sourceRecord.id);
            
            progress.FailedRecordIds.Add(sourceRecord.id);
            Interlocked.Increment(ref progress.FailedRecords);
        }
        finally
        {
            _semaphore.Release();
        }
    }

    private DestinationRecord TransformRecord(SourceRecord source)
    {
        // Implement your transformation logic here
        return new DestinationRecord
        {
            id = source.id,
            FullName = source.Name?.ToUpperInvariant() ?? "UNKNOWN",
            EmailAddress = source.Email?.ToLowerInvariant(),
            ProcessedDate = DateTime.UtcNow,
            IsActive = source.Status?.Equals("Active", StringComparison.OrdinalIgnoreCase) == true,
            TransformationVersion = "1.0",
            Metadata = new Dictionary<string, object>
            {
                { "OriginalCreatedDate", source.CreatedDate },
                { "OriginalStatus", source.Status },
                { "MigrationTimestamp", DateTime.UtcNow }
            }
        };
    }

    private async Task SaveProgressAsync(MigrationProgress progress)
    {
        try
        {
            await _progressContainer.UpsertItemAsync(progress, new PartitionKey(progress.id));
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Failed to save migration progress");
        }
    }

    public async Task<MigrationProgress> GetProgressAsync()
    {
        try
        {
            var response = await _progressContainer.ReadItemAsync<MigrationProgress>(
                "migration_progress", new PartitionKey("migration_progress"));
            return response.Resource;
        }
        catch (CosmosException ex) when (ex.StatusCode == System.Net.HttpStatusCode.NotFound)
        {
            return null;
        }
    }

    public async Task<bool> ReprocessFailedRecordsAsync()
    {
        var progress = await GetProgressAsync();
        if (progress?.FailedRecordIds?.Any() != true)
        {
            _logger.LogInformation("No failed records to reprocess");
            return true;
        }

        _logger.LogInformation("Reprocessing {Count} failed records", progress.FailedRecordIds.Count);
        
        var reprocessedCount = 0;
        var stillFailedIds = new List<string>();

        foreach (var failedId in progress.FailedRecordIds)
        {
            try
            {
                var sourceRecord = await _sourceContainer.ReadItemAsync<SourceRecord>(
                    failedId, new PartitionKey(failedId));
                
                var transformedRecord = TransformRecord(sourceRecord.Resource);
                await _destinationContainer.UpsertItemAsync(transformedRecord, 
                    new PartitionKey(transformedRecord.id));
                
                reprocessedCount++;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to reprocess record {Id}", failedId);
                stillFailedIds.Add(failedId);
            }
        }

        // Update progress
        progress.ProcessedRecords += reprocessedCount;
        progress.FailedRecords = stillFailedIds.Count;
        progress.FailedRecordIds = stillFailedIds;
        progress.Status = stillFailedIds.Count == 0 ? "Completed" : "Completed with errors";
        
        await SaveProgressAsync(progress);
        
        _logger.LogInformation("Reprocessing completed. Success: {Success}, Still failed: {Failed}", 
            reprocessedCount, stillFailedIds.Count);
        
        return stillFailedIds.Count == 0;
    }

    public void Dispose()
    {
        _cosmosClient?.Dispose();
        _semaphore?.Dispose();
    }
}

// Console application example
public class Program
{
    public static async Task Main(string[] args)
    {
        var config = new CosmosConfig
        {
            ConnectionString = "your-cosmos-connection-string",
            DatabaseName = "your-database-name",
            SourceContainerName = "source-container",
            DestinationContainerName = "destination-container",
            BatchSize = 100,
            MaxConcurrency = 10
        };

        using var loggerFactory = LoggerFactory.Create(builder => builder.AddConsole());
        var logger = loggerFactory.CreateLogger<CosmosMigrationService>();

        var migrationService = new CosmosMigrationService(config, logger);

        try
        {
            var success = await migrationService.MigrateDataAsync();
            
            if (!success)
            {
                Console.WriteLine("Migration completed with errors. Attempting to reprocess failed records...");
                await migrationService.ReprocessFailedRecordsAsync();
            }
            
            var finalProgress = await migrationService.GetProgressAsync();
            Console.WriteLine($"Final Status: {finalProgress.Status}");
            Console.WriteLine($"Total: {finalProgress.TotalRecords}, Processed: {finalProgress.ProcessedRecords}, Failed: {finalProgress.FailedRecords}");
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Migration failed: {ex.Message}");
        }
        finally
        {
            migrationService.Dispose();
        }
    }
}
