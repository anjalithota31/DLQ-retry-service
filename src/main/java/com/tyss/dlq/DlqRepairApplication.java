package com.tyss.dlq;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.conn.ssl.TrustAllStrategy;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.ssl.SSLContextBuilder;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.ElasticsearchTransport;
import co.elastic.clients.transport.rest_client.RestClientTransport;

import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * DLQ Repair Service.
 *
 * Flow per poll batch:
 *   1. Repair all records in-memory against the ES mapping
 *   2. Bulk index repaired docs → target index         (1 ES call)
 *   3. Bulk index raw fallback docs → raw index        (1 ES call, only if unconvertible fields exist)
 *   4. Any doc that still fails ES indexing → stored in dlq-failed-documents index for manual review
 *   5. commitAsync()
 *
 * No retry topic — schema failures are deterministic and won't self-heal on retry.
 * Transient ES failures are handled by the bulk retry built into the ES client.
 */
public class DlqRepairApplication {

    private static final Logger log = LoggerFactory.getLogger(DlqRepairApplication.class);

    public static void main(String[] args) throws Exception {
        log.info("Starting DLQ Repair Service");

        Properties appConfig = loadConfig();
        System.out.println("max.retries = " + appConfig.getProperty("max.retries"));
        // Validate configuration before starting
        try {
            ConfigValidator.validate(appConfig);
        } catch (IllegalArgumentException e) {
            log.error("Configuration validation failed: {}", e.getMessage());
            System.exit(1);
        }

        // Helper method to get config from env var or properties file
        java.util.function.Function<String, String> getConfig = (key) -> {
            // Convert property key to env var name (e.g., kafka.bootstrap.servers -> KAFKA_BOOTSTRAP_SERVERS)
            String envVarName = key.toUpperCase().replace('.', '_');
            String envValue = System.getenv(envVarName);
            if (envValue != null && !envValue.isEmpty()) {
                return envValue;
            }
            return appConfig.getProperty(key);
        };

        String dlqTopicPattern  = getConfig.apply("dlq.topic.pattern");
        String dlqTopic         = getConfig.apply("dlq.topic");
        String failedIndex      = getConfig.apply("failed.docs.index");
        final String finalFailedIndex = (failedIndex == null) ? "dlq-failed-documents" : failedIndex;
        String esUrl            = getConfig.apply("elasticsearch.url");
        final String finalEsUrl = (esUrl == null) ? "http://localhost:9200" : esUrl;
        String targetIndex      = getConfig.apply("target.index");
        
        // Mapping cache configuration
        int mappingCacheSize = Integer.parseInt(
                getConfig.apply("mapping.cache.size"));
        if (mappingCacheSize == 0) mappingCacheSize = 100;
        long mappingCacheTtlMinutes = Long.parseLong(
                getConfig.apply("mapping.cache.ttl.minutes"));
        if (mappingCacheTtlMinutes == 0) mappingCacheTtlMinutes = 5;
        
        // Thread pool configuration
        int threadPoolSize = Integer.parseInt(
                getConfig.apply("thread.pool.size"));
        if (threadPoolSize == 0) threadPoolSize = 10;
        int threadPoolQueueSize = Integer.parseInt(
                getConfig.apply("thread.pool.queue.size"));
        if (threadPoolQueueSize == 0) threadPoolQueueSize = 1000;
        
        // Retry configuration
        int maxRetries = Integer.parseInt(getConfig.apply("max.retries"));
        if (maxRetries == 0) maxRetries = 3;
        final int maxConversionRetries = Integer.parseInt(getConfig.apply("max.conversion.retries")) == 0 ? 3 : Integer.parseInt(getConfig.apply("max.conversion.retries"));
        final int maxFieldRemovalRetries = Integer.parseInt(getConfig.apply("max.field.removal.retries")) == 0 ? 5 : Integer.parseInt(getConfig.apply("max.field.removal.retries"));
        long retryBackoffMs = Long.parseLong(getConfig.apply("retry.backoff.ms"));
        if (retryBackoffMs == 0) retryBackoffMs = 100;
        
        // Kafka connection retry configuration
        int kafkaConnectionMaxRetries = Integer.parseInt(getConfig.apply("kafka.connection.max.retries"));
        if (kafkaConnectionMaxRetries == 0) kafkaConnectionMaxRetries = 5;
        long kafkaConnectionRetryBackoffMs = Long.parseLong(getConfig.apply("kafka.connection.retry.backoff.ms"));
        if (kafkaConnectionRetryBackoffMs == 0) kafkaConnectionRetryBackoffMs = 2000;
        
        // Circuit breaker configuration
        int circuitBreakerThreshold = Integer.parseInt(
                getConfig.apply("circuit.breaker.failure.threshold"));
        if (circuitBreakerThreshold == 0) circuitBreakerThreshold = 5;
        long circuitBreakerTimeoutMs = Long.parseLong(
                getConfig.apply("circuit.breaker.timeout.ms"));
        if (circuitBreakerTimeoutMs == 0) circuitBreakerTimeoutMs = 60000;
        
        // Health check configuration
        int healthCheckPort = Integer.parseInt(getConfig.apply("health.check.port"));
        if (healthCheckPort == 0) healthCheckPort = 8080;
        HealthCheckServer healthCheckServer = new HealthCheckServer(healthCheckPort);
        
        // Rate limiter configuration (optional, 0 means disabled)
        double maxRecordsPerSecond = Double.parseDouble(
                getConfig.apply("rate.limit.records.per.second"));
        if (maxRecordsPerSecond == 0) maxRecordsPerSecond = 0;
        RateLimiter rateLimiter = maxRecordsPerSecond > 0 ? new RateLimiter(maxRecordsPerSecond) : null;
        
        try {
            healthCheckServer.start();
        } catch (IOException e) {
            log.warn("Failed to start health check server on port {}", healthCheckPort, e);
        }

        // Credentials: prefer environment variables, fall back to properties file
        String esUser = System.getenv("ES_USERNAME");
        if (esUser == null) esUser = getConfig.apply("elasticsearch.username");
        if (esUser == null) esUser = "";
        String esPass = System.getenv("ES_PASSWORD");
        if (esPass == null) esPass = getConfig.apply("elasticsearch.password");
        if (esPass == null) esPass = "";
        boolean sslVerify = Boolean.parseBoolean(
                getConfig.apply("elasticsearch.ssl.verify"));
        if (getConfig.apply("elasticsearch.ssl.verify") == null) sslVerify = true;

        ElasticsearchClient esClient = buildEsClient(finalEsUrl, esUser, esPass, sslVerify);
        log.info("Elasticsearch client built. url={}, auth={}, sslVerify={}",
                finalEsUrl, !esUser.isEmpty(), sslVerify);

        // Create failed documents index if it doesn't exist
        createFailedDocumentsIndex(esClient, finalFailedIndex);

        ObjectMapper mapper = new ObjectMapper();

        // Initialize mapping cache for dynamic multi-index support
        MappingCache mappingCache = new MappingCache(esClient, mappingCacheSize, mappingCacheTtlMinutes);
        log.info("Mapping cache initialized with size={}, ttl={} minutes", mappingCacheSize, mappingCacheTtlMinutes);
        
        // In multi-topic mode, mappings are loaded dynamically per index from headers
        // No initial mapping needed - will be loaded per target index during processing

        // Create circuit breaker and indexer with retry configuration
        CircuitBreaker circuitBreaker = new CircuitBreaker(circuitBreakerThreshold, circuitBreakerTimeoutMs);
        ElasticsearchIndexer indexer = new ElasticsearchIndexer(esClient, mapper, maxRetries, retryBackoffMs, circuitBreaker);
        
        // Create metrics collector
        MetricsCollector metrics = new MetricsCollector();
        
        // Update health check with circuit breaker status
        healthCheckServer.setHealthMessage("Service initialized, circuit breaker: " + circuitBreaker.getState());

        KafkaConsumer<String, String> consumer = buildConsumerWithRetry(appConfig, kafkaConnectionMaxRetries, kafkaConnectionRetryBackoffMs);

        System.out.println("====================================");
        System.out.println("Consumer created");
        System.out.println("Bootstrap Servers : " + getConfig.apply("kafka.bootstrap.servers"));
        System.out.println("Group Id          : " + getConfig.apply("kafka.consumer.group.id"));
        System.out.println("DLQ Topic         : " + dlqTopic);
        System.out.println("DLQ Pattern       : " + dlqTopicPattern);
        System.out.println("====================================");

        // Subscribe to topics
        if (dlqTopicPattern != null && !dlqTopicPattern.isEmpty()) {
            List<String> matchedTopics = discoverDlqTopics(consumer, dlqTopicPattern);
            if (!matchedTopics.isEmpty()) {
                // Check topic metadata before subscribing
                List<String> validTopics = new ArrayList<>();
                for (String topic : matchedTopics) {
                    try {
                        var partitions = consumer.partitionsFor(topic);
                        System.out.println("Topic '" + topic + "' has " + partitions.size() + " partitions");
                        if (partitions.isEmpty()) {
                            System.out.println("WARNING: Topic '" + topic + "' has 0 partitions!");
                        } else {
                            validTopics.add(topic);
                        }
                    } catch (Exception e) {
                        System.out.println("Error getting partitions for topic '" + topic + "': " + e.getMessage());
                        System.out.println("Skipping this topic due to metadata fetch failure");
                    }
                }
                
                if (!validTopics.isEmpty()) {
                    consumer.subscribe(validTopics);
                    log.info("Subscribed to topics: {}", validTopics);
                } else {
                    System.out.println("All pattern-matched topics failed metadata fetch");
                    System.out.println("Fallback topic configured: " + dlqTopic);
                    try {
                        System.out.println("Available topics: " + consumer.listTopics().keySet());
                    } catch (Exception e) {
                        System.out.println("Error listing topics: " + e.getMessage());
                    }
                    if (dlqTopic != null && !dlqTopic.isEmpty()) {
                        System.out.println("Subscribing to fallback topic: " + dlqTopic);
                        consumer.subscribe(Collections.singletonList(dlqTopic));
                        log.info("Subscribed to fallback topic: {}", dlqTopic);
                        
                        try {
                            var partitions = consumer.partitionsFor(dlqTopic);
                            System.out.println("Fallback topic '" + dlqTopic + "' has " + partitions.size() + " partitions");
                        } catch (Exception e) {
                            System.out.println("Error getting partitions for fallback topic '" + dlqTopic + "': " + e.getMessage());
                        }
                    } else {
                        throw new IllegalStateException("All pattern-matched topics failed metadata fetch and no fallback topic configured");
                    }
                }
            } else {
                System.out.println("No topics matched pattern: " + dlqTopicPattern);
                System.out.println("Available topics: " + consumer.listTopics().keySet());
                if (dlqTopic != null && !dlqTopic.isEmpty()) {
                    System.out.println("Subscribing to fallback topic: " + dlqTopic);
                    consumer.subscribe(Collections.singletonList(dlqTopic));
                    log.info("Subscribed to fallback topic: {}", dlqTopic);
                    
                    try {
                        var partitions = consumer.partitionsFor(dlqTopic);
                        System.out.println("Fallback topic '" + dlqTopic + "' has " + partitions.size() + " partitions");
                    } catch (Exception e) {
                        System.out.println("Error getting partitions for fallback topic '" + dlqTopic + "': " + e.getMessage());
                    }
                } else {
                    throw new IllegalStateException("No topics matched pattern and no fallback topic configured");
                }
            }
        } else if (dlqTopic != null && !dlqTopic.isEmpty()) {
            System.out.println("Subscribing to topic: " + dlqTopic);
            consumer.subscribe(Collections.singletonList(dlqTopic));
            log.info("Subscribed to topic: {}", dlqTopic);
            
            try {
                var partitions = consumer.partitionsFor(dlqTopic);
                System.out.println("Topic '" + dlqTopic + "' has " + partitions.size() + " partitions");
            } catch (Exception e) {
                System.out.println("Error getting partitions for topic '" + dlqTopic + "': " + e.getMessage());
            }
        } else {
            throw new IllegalStateException("Neither dlq.topic.pattern nor dlq.topic is configured");
        }

// Wait until Kafka assigns partitions
        int attempts = 0;

        while (consumer.assignment().isEmpty()) {

            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

            System.out.println("------------------------------------");
            System.out.println("Poll Attempt : " + (++attempts));
            System.out.println("Subscription : " + consumer.subscription());
            System.out.println("Assignment   : " + consumer.assignment());
            System.out.println("Records      : " + records.count());
            System.out.println("------------------------------------");

            if (attempts >= 200) {
                System.out.println("No partition assignment after 200 polls.");
                System.out.println("Please check Kafka broker and consumer group coordinator status.");
                System.out.println("Group ID: " + appConfig.getProperty("kafka.consumer.group.id"));
                System.out.println("Bootstrap Servers: " + appConfig.getProperty("kafka.bootstrap.servers"));
                throw new IllegalStateException("Kafka consumer group coordinator not assigning partitions");
            }
        }
        System.out.println("Final Assignment : " + consumer.assignment());
        log.info("Assigned partitions: {}", consumer.assignment());

// Force consumer to start from beginning to reprocess all DLQ messages
consumer.seekToBeginning(consumer.assignment());

        // Initialize thread pool for concurrent processing
        ExecutorService threadPool = new ThreadPoolExecutor(
                threadPoolSize,
                threadPoolSize,
                60L, TimeUnit.SECONDS,
                new LinkedBlockingQueue<>(threadPoolQueueSize),
                new ThreadPoolExecutor.CallerRunsPolicy());
        log.info("Thread pool initialized with size={}, queueSize={}", threadPoolSize, threadPoolQueueSize);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Shutdown signal received. Flushing and closing.");
            log.info("Final metrics: {}", metrics.getMetricsSummary());
            log.info("Final mapping cache stats: {}", mappingCache.getStats());
            healthCheckServer.setHealthy(false);
            healthCheckServer.setHealthMessage("Shutting down");
            try { consumer.commitSync(); } catch (Exception ignored) {}
            consumer.close();

            // Graceful shutdown of thread pool
            threadPool.shutdown();
            try {
                if (!threadPool.awaitTermination(30, TimeUnit.SECONDS)) {
                    log.warn("Thread pool did not terminate in 30 seconds, forcing shutdown");
                    threadPool.shutdownNow();
                    if (!threadPool.awaitTermination(10, TimeUnit.SECONDS)) {
                        log.error("Thread pool did not terminate after forced shutdown");
                    }
                } else {
                    log.info("Thread pool terminated gracefully");
                }
            } catch (InterruptedException ie) {
                log.warn("Thread pool shutdown interrupted", ie);
                threadPool.shutdownNow();
                Thread.currentThread().interrupt();
            }

            try {
                esClient.close();
            } catch (IOException e) {
                log.warn("Error closing ES client", e);
            }
            healthCheckServer.stop();
        }));

        while (true) {

            // ----------------------------------------------------------------
            // Periodic health check update
            // ----------------------------------------------------------------
            healthCheckServer.setHealthy(circuitBreaker.getState() != CircuitBreaker.State.OPEN);
            healthCheckServer.setHealthMessage(String.format("Operating normally. Circuit breaker: %s, Failures: %d. Cache: %s. %s", 
                    circuitBreaker.getState(), circuitBreaker.getFailureCount(), mappingCache.getStats(), metrics.getMetricsSummary()));

            // ----------------------------------------------------------------
            // Poll
            // ----------------------------------------------------------------
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(2));
            log.info("Polled {} records", records.count());
            if (records.isEmpty()) {
                log.info("No records available");
                continue;
            }

            log.info("Polled {} records from DLQ topic.", records.count());
            
            // Apply rate limiting if configured
            if (rateLimiter != null) {
                try {
                    rateLimiter.acquire(records.count());
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    log.warn("Rate limiting interrupted", e);
                    break;
                }
            }
            
            long batchStartTime = System.currentTimeMillis();
            metrics.incrementRecordsProcessed();

            // ----------------------------------------------------------------
            // Phase 1: Group records by target index for efficient bulk operations
            // ----------------------------------------------------------------

            // Group records by target index for efficient bulk operations
            Map<String, List<ConsumerRecord<String, String>>> recordsByIndex = new HashMap<>();
            for (ConsumerRecord<String, String> record : records) {
                String recordTargetIndex = extractTargetIndex(record);
                log.debug("Record from topic '{}' assigned to target index '{}'", record.topic(), recordTargetIndex);
                recordsByIndex.computeIfAbsent(recordTargetIndex, k -> new ArrayList<>()).add(record);
            }
            
            log.info("Processing {} records across {} target indices: {}", records.count(), recordsByIndex.size(), recordsByIndex.keySet());
            
            // Process each index group concurrently
            List<Future<Void>> futures = new ArrayList<>();
            for (Map.Entry<String, List<ConsumerRecord<String, String>>> entry : recordsByIndex.entrySet()) {
                String indexTarget = entry.getKey();
                List<ConsumerRecord<String, String>> indexRecords = entry.getValue();
                
                futures.add(threadPool.submit(() -> {
                    try {
                        processIndexBatch(indexTarget, indexRecords, mapper, mappingCache,
                                indexer, finalFailedIndex, metrics, maxConversionRetries, maxFieldRemovalRetries, esClient, circuitBreaker);
                    } catch (Exception e) {
                        log.error("Error processing batch for index '{}'", indexTarget, e);
                        throw new RuntimeException(e);
                    }
                    return null;
                }));
            }
            
            // Wait for all batches to complete
            for (Future<Void> future : futures) {
                try {
                    future.get(5, TimeUnit.MINUTES);
                } catch (TimeoutException e) {
                    log.error("Timeout waiting for batch processing");
                } catch (Exception e) {
                    log.error("Error in batch processing", e);
                }
            }

            // ----------------------------------------------------------------
            // Phase 5: Commit offsets
            // ----------------------------------------------------------------
            consumer.commitAsync((offsets, exception) -> {
                if (exception != null) {
                    log.warn("Async offset commit failed. Will retry on next poll.", exception);
                }
            });
            
            // Record batch processing time
            long processingTime = System.currentTimeMillis() - batchStartTime;
            metrics.recordProcessingTime(processingTime);
            log.debug("Batch processing completed in {}ms", processingTime);
        }
    }

    // -------------------------------------------------------------------------
    // Failed-doc storage helpers
    // -------------------------------------------------------------------------

    /** Called when the document was repaired but ES still rejected it. */
    private static void storeIndexFailure(ConsumerRecord<String, String> record,
                                           ElasticsearchIndexer indexer,
                                           String failedIndex,
                                           String targetIndex,
                                           ObjectMapper mapper) {
        try {
            String esId = toEsId(record.key(), record.value());
            FailedDocument failedDoc = new FailedDocument(
                    targetIndex,
                    esId,
                    "ES rejected document after repair",
                    "ES rejected document after repair");
            indexer.index(failedIndex, esId, mapper.convertValue(failedDoc, Map.class));
        } catch (Exception e) {
            log.error("Could not store index-failure doc. id={}. Record may be lost.", toEsId(record.key(), record.value()), e);
        }
    }

    /** Called when the record could not be parsed or repaired at all. */
    private static void storeParseFailure(ConsumerRecord<String, String> record,
                                           Exception cause,
                                           ElasticsearchIndexer indexer,
                                           String failedIndex,
                                           String targetIndex,
                                           ObjectMapper mapper) {
        try {
            String esId = toEsId(record.key(), record.value());
            
            // Build detailed error message with exception type and message
            String errorType = cause.getClass().getSimpleName();
            String errorMessage = cause.getMessage();
            String detailedError = String.format("%s: %s", errorType, 
                errorMessage != null ? errorMessage : "No error message provided");
            
            // Include stack trace snippet for debugging
            StringBuilder stackTrace = new StringBuilder();
            for (StackTraceElement element : cause.getStackTrace()) {
                stackTrace.append("\n    at ").append(element.toString());
                if (stackTrace.length() > 500) { // Limit stack trace length
                    stackTrace.append("\n    ...");
                    break;
                }
            }
            
            String fullErrorDetails = detailedError + stackTrace.toString();
            
            FailedDocument failedDoc = new FailedDocument(
                    targetIndex,
                    esId,
                    detailedError,
                    fullErrorDetails);
            indexer.index(failedIndex, esId, mapper.convertValue(failedDoc, Map.class));
        } catch (Exception e) {
            log.error("Could not store parse-failure doc. id={}. Record may be lost.", toEsId(record.key(), record.value()), e);
        }
    }

    /** Called when document has unconvertible fields. Returns cleaned doc and failure info for bulk storage. */
    private static UnconvertibleFieldResult handleUnconvertibleFields(ConsumerRecord<String, String> record,
                                                                     RepairResult repairResult,
                                                                     String targetIndex,
                                                                     ObjectMapper mapper) {
        String esId = toEsId(record.key(), record.value());
        
        // Build map of unconvertible field names with their reasons
        Map<String, String> problematicFieldsWithReasons = new LinkedHashMap<>();
        repairResult.getUnconvertibleFields().forEach((fieldName, unconvertibleField) -> 
            problematicFieldsWithReasons.put(fieldName, unconvertibleField.getReason()));
        
        // Build list of unconvertible field names for the failure reason
        StringBuilder reason = new StringBuilder("Unconvertible fields: ");
        problematicFieldsWithReasons.keySet().forEach(field -> 
            reason.append(field).append(", "));
        if (reason.length() > 2) {
            reason.setLength(reason.length() - 2); // Remove trailing comma
        }
        
        // Get original document for debugging
        try {
            @SuppressWarnings("unchecked")
            Map<String, Object> originalDoc = mapper.readValue(record.value(), Map.class);
            
            // Convert original document to JSON string to avoid ES mapping conflicts
            String originalDocJson = mapper.writeValueAsString(originalDoc);
            
            // Create failure document for bulk storage with problematic fields, reasons, and original document
            FailedDocument failedDoc = new FailedDocument(targetIndex, esId, reason.toString(), 
                    problematicFieldsWithReasons, null, originalDocJson);
            
            // Return the document WITHOUT unconvertible fields for indexing to target
            Map<String, Object> cleanedDoc = new LinkedHashMap<>(repairResult.getDocument());
            problematicFieldsWithReasons.keySet().forEach(cleanedDoc::remove);
            
            return new UnconvertibleFieldResult(cleanedDoc, failedDoc, esId);
        } catch (Exception e) {
            log.error("Failed to parse original document for unconvertible field tracking. id={}", esId, e);
            // Fallback without original document
            FailedDocument failedDoc = new FailedDocument(targetIndex, esId, reason.toString(), 
                    problematicFieldsWithReasons);
            Map<String, Object> cleanedDoc = new LinkedHashMap<>(repairResult.getDocument());
            problematicFieldsWithReasons.keySet().forEach(cleanedDoc::remove);
            return new UnconvertibleFieldResult(cleanedDoc, failedDoc, esId);
        }
    }

    /** Helper class to hold unconvertible field processing results. */
    private static class UnconvertibleFieldResult {
        final Map<String, Object> cleanedDocument;
        final FailedDocument failureDocument;
        final String documentId;
        
        UnconvertibleFieldResult(Map<String, Object> cleanedDocument, FailedDocument failureDocument, String documentId) {
            this.cleanedDocument = cleanedDocument;
            this.failureDocument = failureDocument;
            this.documentId = documentId;
        }
    }

    /**
     * Safely converts a Kafka record to a valid Elasticsearch document ID.
     * First attempts to extract from Kafka Connect Struct key format: "Struct{fullDocument.unique=...}"
     * If not found, tries to extract the "unique" field from the document content.
     * If still not found, falls back to using the Kafka record key directly.
     * Handles null keys, Struct objects, and other non-string types.
     * For null keys or missing unique field, generates a deterministic ID based on document content hash.
     * Also handles IDs that exceed Elasticsearch's 512-byte limit by generating hash-based IDs.
     */
    private static String toEsId(Object key, String documentValue) {
        // First, try to extract unique from Kafka Connect Struct key format: "Struct{fullDocument.unique=...}"
        // This takes priority over document content extraction to preserve the full Struct format
        if (key != null) {
            String keyStr = key.toString();
            // Pattern to match: Struct{fullDocument.unique=<value>}
            // Updated pattern to handle the exact format: Struct{fullDocument.unique=optimize_demo_LIC2026757PJT1824_module_dataMOD_DATA1c113210-8141-4b39-8170-9b65b8a2af36}
            java.util.regex.Pattern structPattern = java.util.regex.Pattern.compile("Struct\\{.*?fullDocument\\.unique=([^}]+)\\}");
            java.util.regex.Matcher structMatcher = structPattern.matcher(keyStr);
            if (structMatcher.find()) {
                // Return the full Struct format instead of just the unique value
                if (keyStr.getBytes(java.nio.charset.StandardCharsets.UTF_8).length <= 512) {
                    log.info("Using full Struct key as document ID: {}", keyStr);
                    return keyStr;
                } else {
                    log.warn("Struct key exceeds 512 bytes ({} bytes), generating hash-based ID instead",
                            keyStr.getBytes(java.nio.charset.StandardCharsets.UTF_8).length);
                    try {
                        java.security.MessageDigest digest = java.security.MessageDigest.getInstance("SHA-256");
                        byte[] hash = digest.digest(keyStr.getBytes(java.nio.charset.StandardCharsets.UTF_8));
                        StringBuilder hexString = new StringBuilder();
                        for (byte b : hash) {
                            String hex = Integer.toHexString(0xff & b);
                            if (hex.length() == 1) hexString.append('0');
                            hexString.append(hex);
                        }
                        return "struct_" + hexString.substring(0, 32);
                    } catch (Exception e) {
                        log.warn("Failed to generate hash-based ID for long Struct key, falling back to UUID", e);
                        return java.util.UUID.randomUUID().toString();
                    }
                }
            }
        }
        
        // Second, try to extract the "unique" field from the document content
        if (documentValue != null && !documentValue.isEmpty()) {
            try {
                ObjectMapper mapper = new ObjectMapper();
                @SuppressWarnings("unchecked")
                Map<String, Object> docMap = mapper.readValue(documentValue, Map.class);
                Object uniqueValue = docMap.get("unique");
                if (uniqueValue != null) {
                    String uniqueId = uniqueValue.toString();
                    // Check if the unique field value exceeds 512 bytes
                    if (uniqueId.getBytes(java.nio.charset.StandardCharsets.UTF_8).length <= 512) {
                        log.debug("Using 'unique' field from document as document ID: {}", uniqueId);
                        return uniqueId;
                    } else {
                        log.warn("'unique' field exceeds 512 bytes ({} bytes), generating hash-based ID instead", 
                                uniqueId.getBytes(java.nio.charset.StandardCharsets.UTF_8).length);
                        // Generate hash of the unique field value
                        java.security.MessageDigest digest = java.security.MessageDigest.getInstance("SHA-256");
                        byte[] hash = digest.digest(uniqueId.getBytes(java.nio.charset.StandardCharsets.UTF_8));
                        StringBuilder hexString = new StringBuilder();
                        for (byte b : hash) {
                            String hex = Integer.toHexString(0xff & b);
                            if (hex.length() == 1) hexString.append('0');
                            hexString.append(hex);
                        }
                        return "unique_" + hexString.substring(0, 32);
                    }
                }
            } catch (Exception e) {
                log.debug("Could not extract 'unique' field from document, trying key-based extraction", e);
            }
        }
        
        // Fallback to key-based ID generation
        String candidateId;
        
        if (key == null) {
            // Generate deterministic ID from document content hash for null keys
            // This ensures same document always gets same ID on reprocessing
            try {
                java.security.MessageDigest digest = java.security.MessageDigest.getInstance("SHA-256");
                byte[] hash = digest.digest(documentValue.getBytes(java.nio.charset.StandardCharsets.UTF_8));
                StringBuilder hexString = new StringBuilder();
                for (byte b : hash) {
                    String hex = Integer.toHexString(0xff & b);
                    if (hex.length() == 1) hexString.append('0');
                    hexString.append(hex);
                }
                // Use first 32 chars of hash as ID (128 bits, sufficient for uniqueness)
                candidateId = "nullkey_" + hexString.substring(0, 32);
            } catch (Exception e) {
                log.warn("Failed to generate hash-based ID for null key, falling back to UUID", e);
                candidateId = java.util.UUID.randomUUID().toString();
            }
        } else if (key instanceof String) {
            String strKey = (String) key;
            candidateId = strKey.isEmpty() ? ("emptykey_" + java.util.UUID.randomUUID().toString()) : strKey;
        } else {
            // For non-string keys (e.g., Kafka Connect Struct), convert to JSON string
            try {
                ObjectMapper mapper = new ObjectMapper();
                String jsonKey = mapper.writeValueAsString(key);
                // Remove special characters that are invalid in ES IDs
                candidateId = jsonKey.replaceAll("[^a-zA-Z0-9_-]", "_");
            } catch (Exception e) {
                // Fallback to toString() with sanitization
                String strKey = key.toString().replaceAll("[^a-zA-Z0-9_-]", "_");
                candidateId = strKey.isEmpty() ? ("emptykey_" + java.util.UUID.randomUUID().toString()) : strKey;
            }
        }
        
        // Check if ID exceeds Elasticsearch's 512-byte limit
        if (candidateId.getBytes(java.nio.charset.StandardCharsets.UTF_8).length > 512) {
            log.warn("Document ID exceeds 512 bytes ({} bytes), generating hash-based ID instead. Original ID length: {}", 
                    candidateId.getBytes(java.nio.charset.StandardCharsets.UTF_8).length, candidateId.length());
            try {
                java.security.MessageDigest digest = java.security.MessageDigest.getInstance("SHA-256");
                byte[] hash = digest.digest(candidateId.getBytes(java.nio.charset.StandardCharsets.UTF_8));
                StringBuilder hexString = new StringBuilder();
                for (byte b : hash) {
                    String hex = Integer.toHexString(0xff & b);
                    if (hex.length() == 1) hexString.append('0');
                    hexString.append(hex);
                }
                // Use first 32 chars of hash as ID (128 bits, sufficient for uniqueness)
                return "longid_" + hexString.substring(0, 32);
            } catch (Exception e) {
                log.warn("Failed to generate hash-based ID for long ID, falling back to UUID", e);
                return java.util.UUID.randomUUID().toString();
            }
        }
        
        return candidateId;
    }

    /**
     * Extracts the target index from the record headers or derives it from the topic name.
     */
    private static String extractTargetIndex(ConsumerRecord<String, String> record) {
        // First, try to extract from headers
        Headers headers = record.headers();
        for (Header header : headers) {
            if (header.key().equals("X-Target-Index")) {
                String index = new String(header.value(), StandardCharsets.UTF_8).toLowerCase();
                log.debug("Extracted target index from header: {}", index);
                return index;
            }
        }
        
        // Fallback: derive from topic name by removing dlq_ prefix (case-insensitive)
        String topic = record.topic();
        String index = topic.replaceFirst("(?i)^dlq_", "").toLowerCase();
        log.debug("Derived target index from topic '{}': {}", topic, index);
        return index;
    }
    
    /**
     * Processes a batch of records for a specific target index.
     */
    private static void processIndexBatch(String targetIndex,
                                          List<ConsumerRecord<String, String>> records,
                                          ObjectMapper mapper,
                                          MappingCache mappingCache,
                                          ElasticsearchIndexer indexer,
                                          String failedIndex,
                                          MetricsCollector metrics,
                                          int maxConversionRetries,
                                          int maxFieldRemovalRetries,
                                          ElasticsearchClient esClient,
                                          CircuitBreaker circuitBreaker) {
        
        log.info("Starting batch processing for index '{}'. Total records: {}", targetIndex, records.size());
        
        // Ensure target index exists before processing
        createTargetIndexIfNeeded(esClient, targetIndex);
        
        // Get mapping for this index
        Map<String, Object> mapping = mappingCache.getMapping(targetIndex);
        DocumentRepairer indexRepairer = new DocumentRepairer(mapping, mapper, maxConversionRetries);
        
        List<ElasticsearchIndexer.BulkEntry> targetBatch = new ArrayList<>();
        List<ElasticsearchIndexer.FailureEntry> failureBatch = new ArrayList<>();
        Map<String, PendingRecord> pendingById = new LinkedHashMap<>();
        int parseFailureCount = 0;
        
        // Phase 1: Repair all records in-memory
        for (ConsumerRecord<String, String> record : records) {
            try {
                @SuppressWarnings("unchecked")
                Map<String, Object> originalDoc = mapper.readValue(record.value(), Map.class);

                RepairResult result = indexRepairer.repair(originalDoc);

                String esId = toEsId(record.key(), record.value());
                log.info("Processing document. id={}, topic={}, partition={}, offset={}",
                        esId, record.topic(), record.partition(), record.offset());
                log.debug("Repaired record. id={}, repaired={}, removed={}, unconvertible={}",
                        esId,
                        result.getRepairedFields().size(),
                        result.getRemovedFields().size(),
                        result.getUnconvertibleFields().size());

                if (!result.getRepairedFields().isEmpty()) {
                    metrics.incrementRecordsRepaired();
                }
                
                if (!result.getUnconvertibleFields().isEmpty()) {
                    metrics.incrementRecordsWithUnconvertibleFields();
                }

                // If document has unconvertible fields, collect failure info and use cleaned doc for target index
                Map<String, Object> docForTargetIndex = result.getDocument();
                if (!result.getUnconvertibleFields().isEmpty()) {
                    UnconvertibleFieldResult unconvertibleResult = handleUnconvertibleFields(record, result, targetIndex, mapper);
                    docForTargetIndex = unconvertibleResult.cleanedDocument;
                    failureBatch.add(new ElasticsearchIndexer.FailureEntry(
                            unconvertibleResult.documentId,
                            mapper.convertValue(unconvertibleResult.failureDocument, Map.class)));
                }

                targetBatch.add(new ElasticsearchIndexer.BulkEntry(esId, docForTargetIndex));
                pendingById.put(esId, new PendingRecord(record, result, docForTargetIndex));

            } catch (Exception e) {
                // Malformed JSON or unexpected repair crash — store directly in failed-docs
                parseFailureCount++;
                
                // Log detailed information about the failed record
                String recordValuePreview = record.value() != null 
                    ? (record.value().length() > 200 ? record.value().substring(0, 200) + "..." : record.value())
                    : "null";
                
                log.error("Failed to parse/repair DLQ record. offset={}, key={}, topic={}, partition={}, value_preview='{}'. Storing in failed-docs index. Exception type: {}, message: {}",
                        record.offset(), record.key(), record.topic(), record.partition(), 
                        recordValuePreview, e.getClass().getSimpleName(), e.getMessage(), e);
                
                storeParseFailure(record, e, indexer, failedIndex, targetIndex, mapper);
            }
        }
        
        log.info("Phase 1 complete. Repaired: {}, Parse failures: {}, Unconvertible field failures: {}", 
                targetBatch.size(), parseFailureCount, failureBatch.size());
        
        // Phase 2: Bulk index repaired docs → target index
        Map<String, String> failedIdsWithReasons = indexer.bulkIndex(targetIndex, targetBatch);
        Set<String> failedSet = new HashSet<>(failedIdsWithReasons.keySet());

        metrics.incrementEsBulkOperations();
        metrics.incrementRecordsSucceeded(targetBatch.size() - failedSet.size());
        metrics.incrementRecordsFailed(failedSet.size());

        log.info("Bulk index complete for index '{}'. total={}, succeeded={}, failed={}",
                targetIndex, targetBatch.size(), targetBatch.size() - failedSet.size(), failedSet.size());

        // Phase 3: Track ALL documents in dlq-documents-status with SUCCESS/FAILED status and repair details
        List<ElasticsearchIndexer.FailureEntry> allDocumentsStatusBatch = new ArrayList<>();
        
        // Log circuit breaker state before status tracking
        log.info("Phase 3: Circuit breaker state before status tracking: {}", circuitBreaker.getState());
        
        for (ElasticsearchIndexer.BulkEntry entry : targetBatch) {
            PendingRecord pending = pendingById.get(entry.id);
            if (pending == null) continue;
            
            String status = failedSet.contains(entry.id) ? "FAILED" : "SUCCESS";
            String failureReason = failedSet.contains(entry.id) ? failedIdsWithReasons.get(entry.id) : null;
            
            // Build repaired fields map
            Map<String, String> repairedFieldsMap = new LinkedHashMap<>();
            if (!pending.repairResult.getRepairedFields().isEmpty()) {
                for (RepairAction repair : pending.repairResult.getRepairedFields()) {
                    repairedFieldsMap.put(repair.getFieldName(), repair.getStrategy());
                }
            }
            
            // Build removed fields map
            Map<String, String> removedFieldsMap = new LinkedHashMap<>();
            if (!pending.repairResult.getRemovedFields().isEmpty()) {
                for (RemovedField removed : pending.repairResult.getRemovedFields()) {
                    removedFieldsMap.put(removed.getFieldName(), removed.getReason());
                }
            }
            
            // Build problematic fields map
            Map<String, String> problematicFieldsMap = new LinkedHashMap<>();
            if (!pending.repairResult.getUnconvertibleFields().isEmpty()) {
                pending.repairResult.getUnconvertibleFields().forEach((fieldName, unconvertibleField) -> 
                    problematicFieldsMap.put(fieldName, unconvertibleField.getReason()));
            }
            
            // Get original document
            String originalDocJson = null;
            try {
                @SuppressWarnings("unchecked")
                Map<String, Object> originalDoc = mapper.readValue(pending.record.value(), Map.class);
                originalDocJson = mapper.writeValueAsString(originalDoc);
            } catch (Exception e) {
                log.warn("Failed to serialize original document for tracking: {}", entry.id);
            }
            
            // Create status document
            FailedDocument statusDoc = new FailedDocument(
                targetIndex,
                entry.id,
                failureReason,
                problematicFieldsMap,
                repairedFieldsMap,
                removedFieldsMap,
                status.equals("FAILED") ? failedIdsWithReasons.get(entry.id) : null,
                originalDocJson,
                status
            );
            
            allDocumentsStatusBatch.add(new ElasticsearchIndexer.FailureEntry(
                entry.id, mapper.convertValue(statusDoc, Map.class)));
        }
        
        log.info("Phase 3: Total documents to track in dlq-documents-status: {}", allDocumentsStatusBatch.size());
        if (!allDocumentsStatusBatch.isEmpty()) {
            List<String> failedStatusDocs = indexer.bulkIndexFailures(failedIndex, allDocumentsStatusBatch);
            if (failedStatusDocs.isEmpty()) {
                log.info("Bulk indexed {} document status records to '{}'", allDocumentsStatusBatch.size(), failedIndex);
            } else {
                log.error("Failed to index {} document status records to '{}': {}", failedStatusDocs.size(), failedIndex, failedStatusDocs);
                metrics.incrementRecordsFailed(failedStatusDocs.size());
                metrics.incrementFailedDocsIndexFailures(failedStatusDocs.size());
                logToFallbackFile("status_tracking_failed", targetIndex, failedStatusDocs, mapper);
            }
        } else {
            log.warn("Phase 3: No documents to track");
        }

        // Phase 5: Handle ES indexing failures by removing problematic fields and retrying
        log.info("Phase 5: ES indexing failures to handle with field removal and retry: {}", failedIdsWithReasons.size());
        List<ElasticsearchIndexer.FailureEntry> permanentFailures = new ArrayList<>();
        List<ElasticsearchIndexer.FailureEntry> correctedStatusUpdates = new ArrayList<>();

        for (String failedId : failedIdsWithReasons.keySet()) {
            PendingRecord pending = pendingById.get(failedId);
            if (pending == null) {
                log.warn("Pending record not found for failed id={}", failedId);
                continue;
            }
            
            @SuppressWarnings("unchecked")
            Map<String, Object> docForRetry = new LinkedHashMap<>(pending.targetDoc);
            Map<String, String> allRemovedFields = new LinkedHashMap<>();
            boolean successfullyIndexed = false;
            
            // Loop to keep removing problematic fields and retrying
            for (int retryAttempt = 0; retryAttempt < maxFieldRemovalRetries; retryAttempt++) {
                String actualReason = failedIdsWithReasons.get(failedId);
                log.error("ES indexing failed (attempt {}) for id={}, reason={}", retryAttempt + 1, failedId, actualReason);
                
                // Try to extract the problematic field from ES error message
                String problematicField = extractProblematicFieldFromEsError(actualReason);
                
                if (problematicField == null) {
                    log.warn("Could not identify problematic field from ES error for id={}, stopping retry loop", failedId);
                    break;
                }
                
                log.info("Identified problematic field '{}' from ES error for id={}", problematicField, failedId);
                
                // Try to convert the field value before removing it
                boolean conversionAttempted = false;
                boolean conversionSuccessful = false;
                
                // Check if this is a date format error and try conversion
                if (actualReason.toLowerCase().contains("date") || actualReason.toLowerCase().contains("parse")) {
                    Object fieldValue = getNestedFieldValue(docForRetry, problematicField);
                    if (fieldValue != null && fieldValue instanceof String) {
                        String dateValue = (String) fieldValue;
                        // Try to convert date format using DocumentRepairer static method
                        RepairOutcome dateRepair = DocumentRepairer.repairDateStatic(dateValue);
                        if (dateRepair.getStatus() == RepairOutcome.Status.REPAIRED) {
                            Object convertedValue = dateRepair.getRepairedValue();
                            if (setNestedFieldValue(docForRetry, problematicField, convertedValue)) {
                                conversionAttempted = true;
                                conversionSuccessful = true;
                                log.info("Successfully converted date field '{}' from '{}' to '{}'", 
                                        problematicField, dateValue, convertedValue);
                            }
                        }
                    }
                }
                
                // If conversion failed or wasn't attempted, remove the field
                if (!conversionSuccessful) {
                    boolean removed = removeNestedField(docForRetry, problematicField);
                    
                    if (!removed) {
                        log.warn("Could not remove field '{}' from document id={}, stopping retry loop", problematicField, failedId);
                        break;
                    }
                    
                    allRemovedFields.put(problematicField, actualReason);
                    log.info("Removed problematic field '{}' from document id={}, remaining fields: {}", 
                            problematicField, failedId, docForRetry.size());
                } else {
                    log.info("Successfully converted field '{}' instead of removing it", problematicField);
                }
                
                if (docForRetry.isEmpty()) {
                    log.warn("Document became empty after removing field '{}' for id={}", problematicField, failedId);
                    break;
                }
                
                // Retry indexing with the cleaned document
                List<ElasticsearchIndexer.BulkEntry> singleRetryBatch = new ArrayList<>();
                singleRetryBatch.add(new ElasticsearchIndexer.BulkEntry(failedId, docForRetry));
                
                log.info("Retrying indexing for id={} after processing field '{}' (attempt {}/{})", 
                        failedId, problematicField, retryAttempt + 1, maxFieldRemovalRetries);
                
                Map<String, String> retryResult = indexer.bulkIndex(targetIndex, singleRetryBatch);
                
                if (retryResult.isEmpty()) {
                    log.info("Successfully indexed document id={} after processing {} field(s)", 
                            failedId, allRemovedFields.size());
                    successfullyIndexed = true;
                    break; // Success, exit retry loop
                } else {
                    // Update the actual reason for the next iteration
                    String newReason = retryResult.get(failedId);
                    if (newReason != null) {
                        failedIdsWithReasons.put(failedId, newReason);
                        log.warn("Retry failed for id={}, new error: {}", failedId, newReason);
                    }
                }
            }
            
            if (successfullyIndexed) {
                // Document was successfully indexed - update status in status index
                log.info("Document id={} successfully indexed to target index after removing {} field(s)",
                        failedId, allRemovedFields.size());
                metrics.incrementRecordsSucceeded(1);

                // Update the status document with removed fields information
                try {
                    @SuppressWarnings("unchecked")
                    Map<String, Object> originalDoc = mapper.readValue(pending.record.value(), Map.class);
                    String originalDocJson = mapper.writeValueAsString(originalDoc);

                    // Build repaired fields map from original repair result
                    Map<String, String> repairedFieldsMap = new LinkedHashMap<>();
                    if (pending.repairResult != null && !pending.repairResult.getRepairedFields().isEmpty()) {
                        for (RepairAction repair : pending.repairResult.getRepairedFields()) {
                            repairedFieldsMap.put(repair.getFieldName(), repair.getStrategy());
                        }
                    }

                    // Build removed fields map from original repair result + newly removed fields
                    Map<String, String> removedFieldsMap = new LinkedHashMap<>();
                    if (pending.repairResult != null && !pending.repairResult.getRemovedFields().isEmpty()) {
                        for (RemovedField removed : pending.repairResult.getRemovedFields()) {
                            removedFieldsMap.put(removed.getFieldName(), removed.getReason());
                        }
                    }
                    removedFieldsMap.putAll(allRemovedFields);

                    // Build problematic fields map
                    Map<String, String> problematicFieldsMap = new LinkedHashMap<>();
                    if (pending.repairResult != null && !pending.repairResult.getUnconvertibleFields().isEmpty()) {
                        pending.repairResult.getUnconvertibleFields().forEach((fieldName, unconvertibleField) ->
                            problematicFieldsMap.put(fieldName, unconvertibleField.getReason()));
                    }

                    FailedDocument correctedStatusDoc = new FailedDocument(
                        targetIndex, failedId,
                        "Successfully indexed after removing " + allRemovedFields.size() + " problematic field(s)",
                        problematicFieldsMap,
                        repairedFieldsMap,
                        removedFieldsMap,
                        null,
                        originalDocJson,
                        "SUCCESS");
                    correctedStatusUpdates.add(new ElasticsearchIndexer.FailureEntry(
                            failedId, mapper.convertValue(correctedStatusDoc, Map.class)));
                } catch (Exception e) {
                    log.error("Failed to parse original document for corrected status update. id={}", failedId, e);
                }
            } else {
                // Document still failed after max retries, treat as permanent failure
                log.warn("Document id={} failed after {} field removal attempts, treating as permanent failure", 
                        failedId, maxFieldRemovalRetries);
                
                // Build problematicFields map from repair result + removed fields
                Map<String, String> problematicFields = new LinkedHashMap<>();
                if (pending.repairResult != null && !pending.repairResult.getUnconvertibleFields().isEmpty()) {
                    pending.repairResult.getUnconvertibleFields().forEach((fieldName, unconvertibleField) -> 
                        problematicFields.put(fieldName, unconvertibleField.getReason()));
                }
                problematicFields.putAll(allRemovedFields);
                
                // Include original document for debugging
                try {
                    @SuppressWarnings("unchecked")
                    Map<String, Object> originalDoc = mapper.readValue(pending.record.value(), Map.class);
                    String originalDocJson = mapper.writeValueAsString(originalDoc);
                    FailedDocument failedDoc = new FailedDocument(targetIndex, failedId, 
                            failedIdsWithReasons.get(failedId), 
                            problematicFields, failedIdsWithReasons.get(failedId), originalDocJson);
                    permanentFailures.add(new ElasticsearchIndexer.FailureEntry(
                            failedId, mapper.convertValue(failedDoc, Map.class)));
                } catch (Exception e) {
                    log.error("Failed to parse original document for permanent failure tracking. id={}", failedId, e);
                    // Fallback without original document but with problematicFields
                    FailedDocument failedDoc = new FailedDocument(targetIndex, failedId, 
                            failedIdsWithReasons.get(failedId), problematicFields);
                    permanentFailures.add(new ElasticsearchIndexer.FailureEntry(
                            failedId, mapper.convertValue(failedDoc, Map.class)));
                }
            }
        }
        
        log.info("Preparing to index {} permanent failures to '{}'", permanentFailures.size(), failedIndex);
        if (!permanentFailures.isEmpty()) {
            List<String> failedPermanentDocs = indexer.bulkIndexFailures(failedIndex, permanentFailures);
            if (failedPermanentDocs.isEmpty()) {
                log.info("Bulk indexed {} permanent failures to '{}'", permanentFailures.size(), failedIndex);
            } else {
                log.error("Failed to index {} permanent failures to '{}': {}", failedPermanentDocs.size(), failedIndex, failedPermanentDocs);
                metrics.incrementRecordsFailed(failedPermanentDocs.size());
                metrics.incrementFailedDocsIndexFailures(failedPermanentDocs.size());
                logToFallbackFile("permanent_failure_tracking_failed", targetIndex, failedPermanentDocs, mapper);
            }
        } else {
            log.warn("Phase 5: No permanent ES failures to track");
        }

        // Update corrected document status in status index
        log.info("Preparing to update {} corrected document statuses in '{}'", correctedStatusUpdates.size(), failedIndex);
        if (!correctedStatusUpdates.isEmpty()) {
            List<String> failedStatusUpdates = indexer.bulkIndexFailures(failedIndex, correctedStatusUpdates);
            if (failedStatusUpdates.isEmpty()) {
                log.info("Bulk updated {} corrected document statuses in '{}'", correctedStatusUpdates.size(), failedIndex);
            } else {
                log.error("Failed to update {} corrected document statuses in '{}': {}", failedStatusUpdates.size(), failedIndex, failedStatusUpdates);
                metrics.incrementFailedDocsIndexFailures(failedStatusUpdates.size());
                logToFallbackFile("corrected_status_update_failed", targetIndex, failedStatusUpdates, mapper);
            }
        } else {
            log.warn("Phase 5: No corrected document status updates");
        }

        // Summary log for batch processing
        int totalStoredInStatusIndex = allDocumentsStatusBatch.size() + permanentFailures.size() + parseFailureCount + correctedStatusUpdates.size();
        log.info("Batch processing summary for index '{}': Input records={}, Stored in status index={} (Success={}, Failed={}, Permanent ES failures={}, Parse failures={}, With field removal={})",
                targetIndex, records.size(), totalStoredInStatusIndex,
                targetBatch.size() - failedSet.size(), failedSet.size(), permanentFailures.size(), parseFailureCount, correctedStatusUpdates.size());
    }
    
    /**
     * Discovers DLQ topics matching the given regex pattern.
     */
    private static List<String> discoverDlqTopics(KafkaConsumer<String, String> consumer, String pattern) {
        try {
            Pattern regex = Pattern.compile(pattern, Pattern.CASE_INSENSITIVE);;
            return consumer.listTopics().keySet().stream()
                    .filter(topic -> regex.matcher(topic).matches())
                    .sorted()
                    .collect(Collectors.toList());
        } catch (Exception e) {
            log.error("Failed to discover topics matching pattern '{}'", pattern, e);
            return Collections.emptyList();
        }
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    private static class PendingRecord {
        final ConsumerRecord<String, String> record;
        final RepairResult                   repairResult;
        final Map<String, Object>            targetDoc;

        PendingRecord(ConsumerRecord<String, String> record, RepairResult repairResult, Map<String, Object> targetDoc) {
            this.record       = record;
            this.repairResult = repairResult;
            this.targetDoc    = targetDoc;
        }
    }

    private static Properties loadConfig() throws IOException {
        Properties props = new Properties();
        try (InputStream is = DlqRepairApplication.class
                .getClassLoader().getResourceAsStream("dlq-repair.properties")) {
            if (is == null)
                throw new IllegalStateException("dlq-repair.properties not found on classpath");
            props.load(is);
        }
        
        // Resolve ${VAR:default} placeholders
        Properties resolvedProps = new Properties();
        for (String key : props.stringPropertyNames()) {
            String value = props.getProperty(key);
            resolvedProps.setProperty(key, resolvePlaceholders(value));
        }
        return resolvedProps;
    }
    
    private static String resolvePlaceholders(String value) {
        if (value == null || !value.contains("${")) {
            return value;
        }
        
        java.util.regex.Pattern pattern = java.util.regex.Pattern.compile("\\$\\{([^:}]+):([^}]*)\\}");
        java.util.regex.Matcher matcher = pattern.matcher(value);
        StringBuffer result = new StringBuffer();
        
        while (matcher.find()) {
            String envVar = matcher.group(1);
            String defaultValue = matcher.group(2);
            String envValue = System.getenv(envVar);
            String resolvedValue = (envValue != null && !envValue.isEmpty()) ? envValue : defaultValue;
            matcher.appendReplacement(result, java.util.regex.Matcher.quoteReplacement(resolvedValue));
        }
        matcher.appendTail(result);
        
        return result.toString();
    }



    private static ElasticsearchClient buildEsClient(String esUrl,
                                                     String username,
                                                     String password,
                                                     boolean sslVerify) {

        if (esUrl == null || esUrl.trim().isEmpty()) {
            throw new IllegalArgumentException("Elasticsearch URL cannot be null or empty");
        }

        try {
            URI uri = URI.create(esUrl.trim());

            HttpHost httpHost = new HttpHost(
                    uri.getHost(),
                    uri.getPort(),
                    uri.getScheme()
            );

            System.out.println("ES URL        : " + esUrl);
            System.out.println("Parsed Host   : " + uri.getHost());
            System.out.println("Parsed Port   : " + uri.getPort());
            System.out.println("Parsed Scheme : " + uri.getScheme());

            RestClientBuilder builder = RestClient.builder(httpHost);

            boolean hasAuth = username != null && !username.isBlank();
            boolean isHttps = "https".equalsIgnoreCase(uri.getScheme());

            if (hasAuth || (isHttps && !sslVerify)) {
                builder.setHttpClientConfigCallback(httpClientBuilder -> {

                    if (hasAuth) {
                        BasicCredentialsProvider credentialsProvider =
                                new BasicCredentialsProvider();

                        credentialsProvider.setCredentials(
                                AuthScope.ANY,
                                new UsernamePasswordCredentials(username, password)
                        );

                        httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);

                        log.info("Elasticsearch authentication enabled for user '{}'.", username);
                    }

                    if (isHttps && !sslVerify) {
                        try {
                            httpClientBuilder
                                    .setSSLContext(
                                            new SSLContextBuilder()
                                                    .loadTrustMaterial(null, TrustAllStrategy.INSTANCE)
                                                    .build()
                                    )
                                    .setSSLHostnameVerifier(NoopHostnameVerifier.INSTANCE);

                            log.warn("SSL certificate verification disabled.");
                        } catch (Exception e) {
                            throw new IllegalStateException("Failed to build SSL context", e);
                        }
                    }

                    return httpClientBuilder;
                });
            }

            // Create the low-level REST client
            RestClient restClient = builder.build();

            // Create the transport layer with Jackson JSON mapper
            ElasticsearchTransport transport = new RestClientTransport(
                    restClient,
                    new JacksonJsonpMapper()
            );

            // Create the new Elasticsearch client
            return new ElasticsearchClient(transport);

        } catch (Exception e) {
            throw new IllegalArgumentException(
                    "Invalid Elasticsearch URL: " + esUrl, e);
        }
    }

    private static KafkaConsumer<String, String> buildConsumerWithRetry(Properties appConfig, int maxRetries, long initialBackoffMs) {
        int attempt = 0;
        Exception lastException = null;
        
        while (attempt <= maxRetries) {
            try {
                log.info("Attempting to connect to Kafka (attempt {}/{})", attempt + 1, maxRetries + 1);
                KafkaConsumer<String, String> consumer = buildConsumer(appConfig);
                
                // Test the connection by listing topics
                consumer.listTopics();
                log.info("Successfully connected to Kafka on attempt {}", attempt + 1);
                return consumer;
                
            } catch (Exception e) {
                lastException = e;
                log.warn("Kafka connection attempt {} failed: {}", attempt + 1, e.getMessage());
                
                if (attempt < maxRetries) {
                    long backoffMs = initialBackoffMs * (1L << attempt); // Exponential backoff
                    log.info("Retrying Kafka connection in {}ms (exponential backoff)", backoffMs);
                    try {
                        Thread.sleep(backoffMs);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        throw new RuntimeException("Interrupted while waiting to retry Kafka connection", ie);
                    }
                }
                attempt++;
            }
        }
        
        throw new RuntimeException("Failed to connect to Kafka after " + (maxRetries + 1) + " attempts", lastException);
    }

    /**
     * Extracts the problematic field name from an Elasticsearch error message.
     * Handles multiple ES error message formats:
     * - "object mapping for [field.name] tried to parse field [field.name] as object"
     * - "failed to parse field [field.name] of type [text]"
     * - "mapper_parsing_exception: failed to parse [field.name]"
     * - "illegal_argument_exception: mapper [field.name] of different type"
     */
    private static String extractProblematicFieldFromEsError(String esError) {
        if (esError == null || esError.isEmpty()) {
            return null;
        }

        // Pattern 1: Extract field from brackets [field.name] - skip type names
        java.util.regex.Pattern bracketPattern = java.util.regex.Pattern.compile("\\[([^\\]]+)\\]");
        java.util.regex.Matcher bracketMatcher = bracketPattern.matcher(esError);

        while (bracketMatcher.find()) {
            String field = bracketMatcher.group(1);
            // Skip ES type names like "text", "long", "float", "integer", "double", "boolean", "date", "keyword", "object"
            // Only return actual field paths (contain dots or are longer than typical type names)
            if (!field.matches("^(text|long|float|integer|double|boolean|date|keyword|object|mapping)$") &&
                (field.contains(".") || field.matches(".*[a-zA-Z].*"))) {
                log.debug("Extracted field '{}' from ES error (bracket pattern): {}", field, esError);
                return field;
            }
        }

        // Pattern 2: Extract field from "field X" or "for X" patterns
        java.util.regex.Pattern fieldPattern = java.util.regex.Pattern.compile(
            "(?:field|for|mapper)\\s+[`\"']?([\\w\\s.-]+)[`\"']?",
            java.util.regex.Pattern.CASE_INSENSITIVE);
        java.util.regex.Matcher fieldMatcher = fieldPattern.matcher(esError);

        while (fieldMatcher.find()) {
            String field = fieldMatcher.group(1).trim();
            if (field.contains(".") || field.matches(".*[a-zA-Z].*")) {
                log.debug("Extracted field '{}' from ES error (field pattern): {}", field, esError);
                return field;
            }
        }

        // Pattern 3: Extract field from "parsing_exception" messages
        java.util.regex.Pattern parsePattern = java.util.regex.Pattern.compile(
            "parsing.*?([\\w\\s.-]+)",
            java.util.regex.Pattern.CASE_INSENSITIVE);
        java.util.regex.Matcher parseMatcher = parsePattern.matcher(esError);

        while (parseMatcher.find()) {
            String field = parseMatcher.group(1).trim();
            if (field.length() > 2 && (field.contains(".") || field.matches(".*[a-zA-Z].*"))) {
                log.debug("Extracted field '{}' from ES error (parse pattern): {}", field, esError);
                return field;
            }
        }

        log.warn("Could not extract field from ES error: {}", esError);
        return null;
    }

    /**
     * Logs failed document IDs to a fallback file when the failed-documents index is unavailable.
     * This prevents data loss when the tracking index itself fails.
     */
    private static void logToFallbackFile(String failureType, String targetIndex, List<String> failedIds, ObjectMapper mapper) {
        try {
            java.nio.file.Path fallbackDir = java.nio.file.Paths.get("logs", "fallback-tracking");
            java.nio.file.Files.createDirectories(fallbackDir);

            String timestamp = java.time.Instant.now().toString().replace(":", "-");
            java.nio.file.Path fallbackFile = fallbackDir.resolve(failureType + "_" + timestamp + ".json");

            Map<String, Object> fallbackEntry = new LinkedHashMap<>();
            fallbackEntry.put("failureType", failureType);
            fallbackEntry.put("targetIndex", targetIndex);
            fallbackEntry.put("failedIds", failedIds);
            fallbackEntry.put("timestamp", java.time.Instant.now().toString());

            String json = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(fallbackEntry);
            java.nio.file.Files.write(fallbackFile, json.getBytes(java.nio.charset.StandardCharsets.UTF_8));

            log.warn("Logged {} failed IDs to fallback file: {}", failedIds.size(), fallbackFile);
        } catch (Exception e) {
            log.error("Failed to write fallback tracking file for {} failed IDs", failedIds.size(), e);
        }
    }

    /**
     * Gets a nested field value from a document map.
     * Field path format: "parent.child.grandchild"
     * Handles both nested maps and arrays of objects.
     */
    private static Object getNestedFieldValue(Map<String, Object> document, String fieldPath) {
        if (document == null || fieldPath == null || fieldPath.isEmpty()) {
            return null;
        }

        String[] parts = fieldPath.split("\\.");
        if (parts.length == 0) {
            return null;
        }

        // Navigate to the target field
        Object current = document;
        for (String part : parts) {
            if (current instanceof Map) {
                current = ((Map<String, Object>) current).get(part);
            } else if (current instanceof List && !((List<?>) current).isEmpty()) {
                // For arrays, return the first element's value (simplified for now)
                Object firstElement = ((List<?>) current).get(0);
                if (firstElement instanceof Map) {
                    current = ((Map<String, Object>) firstElement).get(part);
                } else {
                    return null;
                }
            } else {
                return null;
            }
            if (current == null) {
                return null;
            }
        }
        return current;
    }

    /**
     * Sets a nested field value in a document map.
     * Field path format: "parent.child.grandchild"
     * Handles both nested maps and arrays of objects.
     */
    private static boolean setNestedFieldValue(Map<String, Object> document, String fieldPath, Object value) {
        if (document == null || fieldPath == null || fieldPath.isEmpty()) {
            return false;
        }

        String[] parts = fieldPath.split("\\.");
        if (parts.length == 0) {
            return false;
        }

        // Navigate to the parent of the target field
        Map<String, Object> current = document;
        for (int i = 0; i < parts.length - 1; i++) {
            Object next = current.get(parts[i]);
            if (next instanceof Map) {
                current = (Map<String, Object>) next;
            } else if (next instanceof List) {
                // Handle array of objects - set value in all elements
                List<Object> list = (List<Object>) next;
                boolean setInAny = false;
                for (Object item : list) {
                    if (item instanceof Map) {
                        Map<String, Object> itemMap = (Map<String, Object>) item;
                        // Build the remaining path for this array element
                        StringBuilder remainingPath = new StringBuilder();
                        for (int j = i + 1; j < parts.length; j++) {
                            if (remainingPath.length() > 0) remainingPath.append(".");
                            remainingPath.append(parts[j]);
                        }
                        if (setNestedFieldValue(itemMap, remainingPath.toString(), value)) {
                            setInAny = true;
                        }
                    }
                }
                return setInAny;
            } else {
                // Path doesn't exist or is not an object
                return false;
            }
        }

        // Set the final field
        String lastPart = parts[parts.length - 1];
        current.put(lastPart, value);
        log.debug("Set field '{}' to value '{}'", fieldPath, value);
        return true;
    }

    /**
     * Removes a nested field from a document map.
     * Field path format: "parent.child.grandchild"
     * Handles both nested maps and arrays of objects.
     */
    private static boolean removeNestedField(Map<String, Object> document, String fieldPath) {
        if (document == null || fieldPath == null || fieldPath.isEmpty()) {
            return false;
        }

        String[] parts = fieldPath.split("\\.");
        if (parts.length == 0) {
            return false;
        }

        // Navigate to the parent of the target field
        Map<String, Object> current = document;
        for (int i = 0; i < parts.length - 1; i++) {
            Object value = current.get(parts[i]);
            if (value instanceof Map) {
                current = (Map<String, Object>) value;
            } else if (value instanceof List) {
                // Handle array of objects - remove field from all elements
                List<Object> list = (List<Object>) value;
                boolean removedFromAny = false;
                for (Object item : list) {
                    if (item instanceof Map) {
                        Map<String, Object> itemMap = (Map<String, Object>) item;
                        // Build the remaining path for this array element
                        StringBuilder remainingPath = new StringBuilder();
                        for (int j = i + 1; j < parts.length; j++) {
                            if (remainingPath.length() > 0) remainingPath.append(".");
                            remainingPath.append(parts[j]);
                        }
                        if (removeNestedField(itemMap, remainingPath.toString())) {
                            removedFromAny = true;
                        }
                    }
                }
                return removedFromAny;
            } else {
                // Path doesn't exist or is not an object
                return false;
            }
        }

        // Remove the final field
        String lastPart = parts[parts.length - 1];
        Object removed = current.remove(lastPart);
        if (removed != null) {
            log.info("Successfully removed field '{}' from document", fieldPath);
            return true;
        }
        log.warn("Field '{}' not found in document", fieldPath);
        return false;
    }

    private static KafkaConsumer<String, String> buildConsumer(Properties appConfig) {
        Properties props = new Properties();
        
        // Helper to get config from env var or properties file
        java.util.function.Function<String, String> getConfig = (key) -> {
            String envVarName = key.toUpperCase().replace('.', '_');
            String envValue = System.getenv(envVarName);
            if (envValue != null && !envValue.isEmpty()) {
                return envValue;
            }
            return appConfig.getProperty(key);
        };
        
        // Prefer environment variables for sensitive Kafka config
        String bootstrapServers = getConfig.apply("kafka.bootstrap.servers");
        if (bootstrapServers == null) bootstrapServers = "localhost:9092";
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        
        String groupId = getConfig.apply("kafka.consumer.group.id");
        if (groupId == null) groupId = "dlq-repair-service";
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,  "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        
        String maxPollRecords = getConfig.apply("consumer.max.poll.records");
        if (maxPollRecords == null) maxPollRecords = "500";
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, maxPollRecords);
        
        // Prevent rebalance if bulk ES indexing takes longer than default 5 min
        String maxPollInterval = getConfig.apply("consumer.max.poll.interval.ms");
        if (maxPollInterval == null) maxPollInterval = "600000";
        props.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, maxPollInterval);
        
        // Session timeout - critical for partition assignment
        String sessionTimeout = getConfig.apply("consumer.session.timeout.ms");
        if (sessionTimeout == null) sessionTimeout = "30000";
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, sessionTimeout);
        
        // Heartbeat interval - must be less than session timeout
        String heartbeatInterval = getConfig.apply("consumer.heartbeat.interval.ms");
        if (heartbeatInterval == null) heartbeatInterval = "10000";
        props.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, heartbeatInterval);
        
        // Metadata request timeout - increase for slow networks
        String metadataTimeout = getConfig.apply("consumer.metadata.timeout.ms");
        if (metadataTimeout == null) metadataTimeout = "30000";
        props.put("metadata.fetch.timeout.ms", metadataTimeout);
        
        // Request timeout for all requests
        String requestTimeout = getConfig.apply("consumer.request.timeout.ms");
        if (requestTimeout == null) requestTimeout = "40000";
        props.put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, requestTimeout);
        
        // Rebalance timeout - time for group rebalance
        String rebalanceTimeout = getConfig.apply("consumer.rebalance.timeout.ms");
        if (rebalanceTimeout == null) rebalanceTimeout = "60000";
        props.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, maxPollInterval); // Keep original max poll interval
        
        // Fetch min bytes - minimum bytes to wait for in a fetch
        String fetchMinBytes = getConfig.apply("consumer.fetch.min.bytes");
        if (fetchMinBytes == null) fetchMinBytes = "1";
        props.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, fetchMinBytes);
        
        // Fetch max wait - max time to wait for fetch.min.bytes
        String fetchMaxWait = getConfig.apply("consumer.fetch.max.wait.ms");
        if (fetchMaxWait == null) fetchMaxWait = "500";
        props.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, fetchMaxWait);
        
        System.out.println("Kafka Consumer Properties:");
        props.forEach((k, v) -> System.out.println(k + " = " + v));
        
        return new KafkaConsumer<>(props);
    }

    /**
     * Creates the failed documents index with proper mappings if it doesn't exist.
     */
    private static void createFailedDocumentsIndex(ElasticsearchClient esClient, String indexName) {
        try {
            // Check if index exists
            boolean exists = esClient.indices().exists(e -> e.index(indexName)).value();
            
            if (exists) {
                log.info("Document status index '{}' already exists", indexName);
                return;
            }
            
            log.info("Creating document status index '{}' with enhanced mappings", indexName);
            
            // Create index with mappings for tracking all document statuses (SUCCESS/FAILED)
            esClient.indices().create(c -> c
                    .index(indexName)
                    .mappings(m -> m
                            .properties("indexName", p -> p.keyword(k -> k))
                            .properties("documentId", p -> p.keyword(k -> k))
                            .properties("status", p -> p.keyword(k -> k))
                            .properties("failureReason", p -> p.text(t -> t))
                            .properties("problematicFields", p -> p.object(o -> o.dynamic(co.elastic.clients.elasticsearch._types.mapping.DynamicMapping.True)))
                            .properties("repairedFields", p -> p.object(o -> o.dynamic(co.elastic.clients.elasticsearch._types.mapping.DynamicMapping.True)))
                            .properties("removedFields", p -> p.object(o -> o.dynamic(co.elastic.clients.elasticsearch._types.mapping.DynamicMapping.True)))
                            .properties("esErrorDetails", p -> p.text(t -> t))
                            .properties("processedAt", p -> p.date(d -> d))
                            .properties("originalDocument", p -> p.text(t -> t))));
            
            log.info("Successfully created document status index '{}'", indexName);
            
        } catch (Exception e) {
            log.error("Failed to create document status index '{}'. Status tracking may fail. Error: {}", indexName, e.getMessage(), e);
            // Don't fail startup - ES may auto-create the index, but log as error for visibility
        }
    }

    /**
     * Creates a target index if it doesn't exist, with dynamic mapping enabled.
     * This ensures DLQ indexes are created for each connector.
     */
    private static void createTargetIndexIfNeeded(ElasticsearchClient esClient, String indexName) {
        try {
            // Check if index exists
            boolean exists = esClient.indices().exists(e -> e.index(indexName)).value();
            
            if (exists) {
                log.debug("Target index '{}' already exists", indexName);
                return;
            }
            
            log.info("Creating target index '{}' with dynamic mapping enabled and increased field limit", indexName);
            
            // Create index with dynamic mapping enabled and increased field limit to handle diverse document structures
            esClient.indices().create(c -> c
                    .index(indexName)
                    .settings(s -> s
                            .mapping(m -> m
                                    .totalFields(tf -> tf.limit("2000"))))
                    .mappings(m -> m
                            .dynamic(co.elastic.clients.elasticsearch._types.mapping.DynamicMapping.True)));
            
            log.info("Successfully created target index '{}' with 2000 field limit", indexName);
            
        } catch (Exception e) {
            log.warn("Failed to create target index '{}'. Will rely on ES auto-creation during indexing.", indexName, e);
            // Don't fail processing - ES may auto-create the index during bulk indexing
        }
    }
}
