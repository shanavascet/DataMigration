// pom.xml dependencies
/*
<dependencies>
    <dependency>
        <groupId>org.springframework.boot</groupId>
        <artifactId>spring-boot-starter</artifactId>
        <version>3.2.0</version>
    </dependency>
    <dependency>
        <groupId>com.azure</groupId>
        <artifactId>azure-cosmos</artifactId>
        <version>4.45.0</version>
    </dependency>
    <dependency>
        <groupId>org.springframework.boot</groupId>
        <artifactId>spring-boot-starter-logging</artifactId>
        <version>3.2.0</version>
    </dependency>
    <dependency>
        <groupId>com.fasterxml.jackson.core</groupId>
        <artifactId>jackson-databind</artifactId>
        <version>2.15.2</version>
    </dependency>
    <dependency>
        <groupId>org.springframework.boot</groupId>
        <artifactId>spring-boot-configuration-processor</artifactId>
        <version>3.2.0</version>
    </dependency>
</dependencies>
*/

// Configuration class
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@Component
@ConfigurationProperties(prefix = "cosmos")
public class CosmosConfig {
    private String connectionString;
    private String databaseName;
    private String sourceContainerName;
    private String destinationContainerName;
    private int batchSize = 100;
    private int maxConcurrency = 10;
    private int maxRetries = 3;
    private int retryDelayMs = 1000;
    
    // Getters and setters
    public String getConnectionString() { return connectionString; }
    public void setConnectionString(String connectionString) { this.connectionString = connectionString; }
    
    public String getDatabaseName() { return databaseName; }
    public void setDatabaseName(String databaseName) { this.databaseName = databaseName; }
    
    public String getSourceContainerName() { return sourceContainerName; }
    public void setSourceContainerName(String sourceContainerName) { this.sourceContainerName = sourceContainerName; }
    
    public String getDestinationContainerName() { return destinationContainerName; }
    public void setDestinationContainerName(String destinationContainerName) { this.destinationContainerName = destinationContainerName; }
    
    public int getBatchSize() { return batchSize; }
    public void setBatchSize(int batchSize) { this.batchSize = batchSize; }
    
    public int getMaxConcurrency() { return maxConcurrency; }
    public void setMaxConcurrency(int maxConcurrency) { this.maxConcurrency = maxConcurrency; }
    
    public int getMaxRetries() { return maxRetries; }
    public void setMaxRetries(int maxRetries) { this.maxRetries = maxRetries; }
    
    public int getRetryDelayMs() { return retryDelayMs; }
    public void setRetryDelayMs(int retryDelayMs) { this.retryDelayMs = retryDelayMs; }
}

// Source record model
import com.fasterxml.jackson.annotation.JsonProperty;
import java.time.LocalDateTime;
import java.util.Map;

public class SourceRecord {
    @JsonProperty("id")
    private String id;
    
    @JsonProperty("name")
    private String name;
    
    @JsonProperty("email")
    private String email;
    
    @JsonProperty("createdDate")
    private LocalDateTime createdDate;
    
    @JsonProperty("status")
    private String status;
    
    @JsonProperty("additionalProperties")
    private Map<String, Object> additionalProperties;
    
    // Constructors
    public SourceRecord() {}
    
    public SourceRecord(String id, String name, String email, LocalDateTime createdDate, String status) {
        this.id = id;
        this.name = name;
        this.email = email;
        this.createdDate = createdDate;
        this.status = status;
    }
    
    // Getters and setters
    public String getId() { return id; }
    public void setId(String id) { this.id = id; }
    
    public String getName() { return name; }
    public void setName(String name) { this.name = name; }
    
    public String getEmail() { return email; }
    public void setEmail(String email) { this.email = email; }
    
    public LocalDateTime getCreatedDate() { return createdDate; }
    public void setCreatedDate(LocalDateTime createdDate) { this.createdDate = createdDate; }
    
    public String getStatus() { return status; }
    public void setStatus(String status) { this.status = status; }
    
    public Map<String, Object> getAdditionalProperties() { return additionalProperties; }
    public void setAdditionalProperties(Map<String, Object> additionalProperties) { this.additionalProperties = additionalProperties; }
}

// Destination record model
import com.fasterxml.jackson.annotation.JsonProperty;
import java.time.LocalDateTime;
import java.util.Map;

public class DestinationRecord {
    @JsonProperty("id")
    private String id;
    
    @JsonProperty("fullName")
    private String fullName;
    
    @JsonProperty("emailAddress")
    private String emailAddress;
    
    @JsonProperty("processedDate")
    private LocalDateTime processedDate;
    
    @JsonProperty("isActive")
    private boolean isActive;
    
    @JsonProperty("transformationVersion")
    private String transformationVersion;
    
    @JsonProperty("metadata")
    private Map<String, Object> metadata;
    
    // Constructors
    public DestinationRecord() {}
    
    // Getters and setters
    public String getId() { return id; }
    public void setId(String id) { this.id = id; }
    
    public String getFullName() { return fullName; }
    public void setFullName(String fullName) { this.fullName = fullName; }
    
    public String getEmailAddress() { return emailAddress; }
    public void setEmailAddress(String emailAddress) { this.emailAddress = emailAddress; }
    
    public LocalDateTime getProcessedDate() { return processedDate; }
    public void setProcessedDate(LocalDateTime processedDate) { this.processedDate = processedDate; }
    
    public boolean isActive() { return isActive; }
    public void setActive(boolean active) { isActive = active; }
    
    public String getTransformationVersion() { return transformationVersion; }
    public void setTransformationVersion(String transformationVersion) { this.transformationVersion = transformationVersion; }
    
    public Map<String, Object> getMetadata() { return metadata; }
    public void setMetadata(Map<String, Object> metadata) { this.metadata = metadata; }
}

// Migration progress model
import com.fasterxml.jackson.annotation.JsonProperty;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

public class MigrationProgress {
    @JsonProperty("id")
    private String id = "migration_progress";
    
    @JsonProperty("totalRecords")
    private int totalRecords;
    
    @JsonProperty("processedRecords")
    private int processedRecords;
    
    @JsonProperty("failedRecords")
    private int failedRecords;
    
    @JsonProperty("startTime")
    private LocalDateTime startTime;
    
    @JsonProperty("endTime")
    private LocalDateTime endTime;
    
    @JsonProperty("status")
    private String status;
    
    @JsonProperty("failedRecordIds")
    private List<String> failedRecordIds = new ArrayList<>();
    
    @JsonProperty("lastProcessedId")
    private String lastProcessedId;
    
    // Constructors
    public MigrationProgress() {}
    
    // Getters and setters
    public String getId() { return id; }
    public void setId(String id) { this.id = id; }
    
    public int getTotalRecords() { return totalRecords; }
    public void setTotalRecords(int totalRecords) { this.totalRecords = totalRecords; }
    
    public int getProcessedRecords() { return processedRecords; }
    public void setProcessedRecords(int processedRecords) { this.processedRecords = processedRecords; }
    
    public int getFailedRecords() { return failedRecords; }
    public void setFailedRecords(int failedRecords) { this.failedRecords = failedRecords; }
    
    public LocalDateTime getStartTime() { return startTime; }
    public void setStartTime(LocalDateTime startTime) { this.startTime = startTime; }
    
    public LocalDateTime getEndTime() { return endTime; }
    public void setEndTime(LocalDateTime endTime) { this.endTime = endTime; }
    
    public String getStatus() { return status; }
    public void setStatus(String status) { this.status = status; }
    
    public List<String> getFailedRecordIds() { return failedRecordIds; }
    public void setFailedRecordIds(List<String> failedRecordIds) { this.failedRecordIds = failedRecordIds; }
    
    public String getLastProcessedId() { return lastProcessedId; }
    public void setLastProcessedId(String lastProcessedId) { this.lastProcessedId = lastProcessedId; }
}

// Migration service
import com.azure.cosmos.*;
import com.azure.cosmos.models.*;
import com.azure.cosmos.util.CosmosPagedIterable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

@Service
public class CosmosMigrationService {
    private static final Logger logger = LoggerFactory.getLogger(CosmosMigrationService.class);
    
    private final CosmosClient cosmosClient;
    private final CosmosContainer sourceContainer;
    private final CosmosContainer destinationContainer;
    private final CosmosContainer progressContainer;
    private final CosmosConfig config;
    private final Semaphore semaphore;
    private final ExecutorService executorService;
    
    @Autowired
    public CosmosMigrationService(CosmosConfig config) {
        this.config = config;
        this.cosmosClient = new CosmosClientBuilder()
                .endpoint(extractEndpointFromConnectionString(config.getConnectionString()))
                .key(extractKeyFromConnectionString(config.getConnectionString()))
                .consistencyLevel(ConsistencyLevel.SESSION)
                .buildClient();
        
        CosmosDatabase database = cosmosClient.getDatabase(config.getDatabaseName());
        this.sourceContainer = database.getContainer(config.getSourceContainerName());
        this.destinationContainer = database.getContainer(config.getDestinationContainerName());
        this.progressContainer = database.getContainer("migration_progress");
        
        this.semaphore = new Semaphore(config.getMaxConcurrency());
        this.executorService = Executors.newFixedThreadPool(config.getMaxConcurrency());
    }
    
    public CompletableFuture<Boolean> migrateDataAsync() {
        return CompletableFuture.supplyAsync(() -> {
            MigrationProgress progress = new MigrationProgress();
            progress.setStartTime(LocalDateTime.now());
            progress.setStatus("Starting");
            
            try {
                logger.info("Starting data migration from {} to {}", 
                    config.getSourceContainerName(), config.getDestinationContainerName());
                
                // Get total count
                progress.setTotalRecords(getTotalRecordCount());
                progress.setStatus("In Progress");
                saveProgress(progress);
                
                logger.info("Total records to migrate: {}", progress.getTotalRecords());
                
                // Process in batches
                processInBatches(progress);
                
                progress.setEndTime(LocalDateTime.now());
                progress.setStatus(progress.getFailedRecords() > 0 ? "Completed with errors" : "Completed");
                saveProgress(progress);
                
                logger.info("Migration completed. Processed: {}, Failed: {}", 
                    progress.getProcessedRecords(), progress.getFailedRecords());
                
                return progress.getFailedRecords() == 0;
                
            } catch (Exception ex) {
                progress.setStatus("Failed");
                progress.setEndTime(LocalDateTime.now());
                saveProgress(progress);
                logger.error("Migration failed", ex);
                throw new RuntimeException("Migration failed", ex);
            }
        }, executorService);
    }
    
    private int getTotalRecordCount() {
        String query = "SELECT VALUE COUNT(1) FROM c";
        CosmosPagedIterable<Integer> queryIterable = sourceContainer.queryItems(query, new CosmosQueryRequestOptions(), Integer.class);
        return queryIterable.iterator().next();
    }
    
    private void processInBatches(MigrationProgress progress) {
        String continuationToken = null;
        AtomicInteger totalProcessed = new AtomicInteger(0);
        AtomicInteger totalFailed = new AtomicInteger(0);
        
        do {
            CosmosQueryRequestOptions queryOptions = new CosmosQueryRequestOptions();
            queryOptions.setMaxItemCount(config.getBatchSize());
            
            String query = "SELECT * FROM c";
            CosmosPagedIterable<SourceRecord> queryIterable = sourceContainer.queryItems(
                query, queryOptions, SourceRecord.class);
            
            List<SourceRecord> batch = new ArrayList<>();
            Iterator<SourceRecord> iterator = queryIterable.iterator();
            
            // Collect batch
            while (iterator.hasNext() && batch.size() < config.getBatchSize()) {
                batch.add(iterator.next());
            }
            
            if (!batch.isEmpty()) {
                // Process batch concurrently
                List<CompletableFuture<ProcessingResult>> futures = new ArrayList<>();
                
                for (SourceRecord record : batch) {
                    futures.add(processRecordAsync(record));
                }
                
                // Wait for all records in batch to complete
                CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
                
                // Collect results
                for (CompletableFuture<ProcessingResult> future : futures) {
                    try {
                        ProcessingResult result = future.get();
                        if (result.isSuccess()) {
                            totalProcessed.incrementAndGet();
                        } else {
                            totalFailed.incrementAndGet();
                            progress.getFailedRecordIds().add(result.getRecordId());
                        }
                    } catch (Exception e) {
                        totalFailed.incrementAndGet();
                        logger.error("Error processing record", e);
                    }
                }
                
                // Update progress
                progress.setProcessedRecords(totalProcessed.get());
                progress.setFailedRecords(totalFailed.get());
                
                // Update progress periodically
                if (totalProcessed.get() % 1000 == 0) {
                    saveProgress(progress);
                    logger.info("Progress: {}/{} records processed", 
                        totalProcessed.get(), progress.getTotalRecords());
                }
            }
            
            // Check if there are more results (simplified approach)
            continuationToken = batch.size() == config.getBatchSize() ? "continue" : null;
            
        } while (continuationToken != null);
    }
    
    private CompletableFuture<ProcessingResult> processRecordAsync(SourceRecord sourceRecord) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                semaphore.acquire();
                
                // Transform the record
                DestinationRecord transformedRecord = transformRecord(sourceRecord);
                
                // Save to destination container with retry logic
                saveWithRetry(transformedRecord);
                
                return new ProcessingResult(sourceRecord.getId(), true, null);
                
            } catch (Exception ex) {
                logger.error("Failed to process record {}", sourceRecord.getId(), ex);
                return new ProcessingResult(sourceRecord.getId(), false, ex.getMessage());
            } finally {
                semaphore.release();
            }
        }, executorService);
    }
    
    private void saveWithRetry(DestinationRecord record) {
        int retryCount = 0;
        Exception lastException = null;
        
        while (retryCount < config.getMaxRetries()) {
            try {
                destinationContainer.upsertItem(record);
                return; // Success
            } catch (CosmosException ex) {
                lastException = ex;
                retryCount++;
                
                if (ex.getStatusCode() == 429) { // Too Many Requests
                    try {
                        Thread.sleep(config.getRetryDelayMs() * retryCount);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        throw new RuntimeException("Interrupted during retry", ie);
                    }
                } else {
                    throw ex; // Non-retryable error
                }
            }
        }
        
        throw new RuntimeException("Max retries exceeded", lastException);
    }
    
    private DestinationRecord transformRecord(SourceRecord source) {
        DestinationRecord destination = new DestinationRecord();
        destination.setId(source.getId());
        destination.setFullName(source.getName() != null ? source.getName().toUpperCase() : "UNKNOWN");
        destination.setEmailAddress(source.getEmail() != null ? source.getEmail().toLowerCase() : null);
        destination.setProcessedDate(LocalDateTime.now());
        destination.setActive("Active".equalsIgnoreCase(source.getStatus()));
        destination.setTransformationVersion("1.0");
        
        // Create metadata
        Map<String, Object> metadata = new HashMap<>();
        metadata.put("originalCreatedDate", source.getCreatedDate());
        metadata.put("originalStatus", source.getStatus());
        metadata.put("migrationTimestamp", LocalDateTime.now());
        destination.setMetadata(metadata);
        
        return destination;
    }
    
    private void saveProgress(MigrationProgress progress) {
        try {
            progressContainer.upsertItem(progress);
        } catch (Exception ex) {
            logger.warn("Failed to save migration progress", ex);
        }
    }
    
    public MigrationProgress getProgress() {
        try {
            CosmosItemResponse<MigrationProgress> response = progressContainer.readItem(
                "migration_progress", 
                new PartitionKey("migration_progress"), 
                MigrationProgress.class);
            return response.getItem();
        } catch (CosmosException ex) {
            if (ex.getStatusCode() == 404) {
                return null;
            }
            throw ex;
        }
    }
    
    public CompletableFuture<Boolean> reprocessFailedRecordsAsync() {
        return CompletableFuture.supplyAsync(() -> {
            MigrationProgress progress = getProgress();
            if (progress == null || progress.getFailedRecordIds().isEmpty()) {
                logger.info("No failed records to reprocess");
                return true;
            }
            
            logger.info("Reprocessing {} failed records", progress.getFailedRecordIds().size());
            
            int reprocessedCount = 0;
            List<String> stillFailedIds = new ArrayList<>();
            
            for (String failedId : progress.getFailedRecordIds()) {
                try {
                    CosmosItemResponse<SourceRecord> sourceResponse = sourceContainer.readItem(
                        failedId, new PartitionKey(failedId), SourceRecord.class);
                    
                    DestinationRecord transformedRecord = transformRecord(sourceResponse.getItem());
                    destinationContainer.upsertItem(transformedRecord);
                    
                    reprocessedCount++;
                } catch (Exception ex) {
                    logger.error("Failed to reprocess record {}", failedId, ex);
                    stillFailedIds.add(failedId);
                }
            }
            
            // Update progress
            progress.setProcessedRecords(progress.getProcessedRecords() + reprocessedCount);
            progress.setFailedRecords(stillFailedIds.size());
            progress.setFailedRecordIds(stillFailedIds);
            progress.setStatus(stillFailedIds.isEmpty() ? "Completed" : "Completed with errors");
            
            saveProgress(progress);
            
            logger.info("Reprocessing completed. Success: {}, Still failed: {}", 
                reprocessedCount, stillFailedIds.size());
            
            return stillFailedIds.isEmpty();
        }, executorService);
    }
    
    public void shutdown() {
        try {
            executorService.shutdown();
            if (!executorService.awaitTermination(60, TimeUnit.SECONDS)) {
                executorService.shutdownNow();
            }
        } catch (InterruptedException e) {
            executorService.shutdownNow();
            Thread.currentThread().interrupt();
        }
        
        if (cosmosClient != null) {
            cosmosClient.close();
        }
    }
    
    // Helper methods
    private String extractEndpointFromConnectionString(String connectionString) {
        // Parse connection string to extract endpoint
        String[] parts = connectionString.split(";");
        for (String part : parts) {
            if (part.startsWith("AccountEndpoint=")) {
                return part.substring("AccountEndpoint=".length());
            }
        }
        throw new IllegalArgumentException("Invalid connection string format");
    }
    
    private String extractKeyFromConnectionString(String connectionString) {
        // Parse connection string to extract key
        String[] parts = connectionString.split(";");
        for (String part : parts) {
            if (part.startsWith("AccountKey=")) {
                return part.substring("AccountKey=".length());
            }
        }
        throw new IllegalArgumentException("Invalid connection string format");
    }
    
    // Helper class for processing results
    private static class ProcessingResult {
        private final String recordId;
        private final boolean success;
        private final String errorMessage;
        
        public ProcessingResult(String recordId, boolean success, String errorMessage) {
            this.recordId = recordId;
            this.success = success;
            this.errorMessage = errorMessage;
        }
        
        public String getRecordId() { return recordId; }
        public boolean isSuccess() { return success; }
        public String getErrorMessage() { return errorMessage; }
    }
}
