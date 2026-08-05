package com.tyss.dlq;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.ErrorCause;
import co.elastic.clients.elasticsearch._types.ErrorResponse;
import co.elastic.clients.elasticsearch.core.BulkResponse;
import co.elastic.clients.elasticsearch.core.IndexRequest;
import co.elastic.clients.elasticsearch.core.bulk.BulkOperation;
import co.elastic.clients.json.JsonData;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Handles indexing documents into Elasticsearch.
 *
 * Two modes:
 *  - Single document : index() / indexAsAllStrings()  — used for failed-docs index
 *  - Batch           : bulkIndex() / bulkIndexAsAllStrings() — used for main poll loop
 */
public class ElasticsearchIndexer {

    private static final Logger log = LoggerFactory.getLogger(ElasticsearchIndexer.class);

    private final ElasticsearchClient client;
    private final ObjectMapper mapper;
    private final int maxRetries;
    private final long initialRetryBackoffMs;
    private final CircuitBreaker circuitBreaker;

    public ElasticsearchIndexer(ElasticsearchClient client, ObjectMapper mapper) {
        this(client, mapper, 3, 100, new CircuitBreaker()); // defaults: 3 retries, 100ms initial backoff
    }

    public ElasticsearchIndexer(ElasticsearchClient client, ObjectMapper mapper,
                                   int maxRetries, long initialRetryBackoffMs) {
        this(client, mapper, maxRetries, initialRetryBackoffMs, new CircuitBreaker());
    }

    public ElasticsearchIndexer(ElasticsearchClient client, ObjectMapper mapper,
                                   int maxRetries, long initialRetryBackoffMs, CircuitBreaker circuitBreaker) {
        this.client = client;
        this.mapper = mapper;
        this.maxRetries = maxRetries;
        this.initialRetryBackoffMs = initialRetryBackoffMs;
        this.circuitBreaker = circuitBreaker;
    }

    // -------------------------------------------------------------------------
    // Single-document methods (used for failed-docs index — low volume)
    // -------------------------------------------------------------------------

    public boolean index(String index, String id, Map<String, Object> document) {
        try {
            String json = mapper.writeValueAsString(document);
            client.index(i -> i
                    .index(index)
                    .id(id)
                    .withJson(new java.io.StringReader(json)));
            log.debug("Indexed document. index={}, id={}", index, id);
            return true;
        } catch (Exception e) {
            log.error("Failed to index document. index={}, id={}", index, id, e);
            return false;
        }
    }

    /**
     * Bulk-indexes failure documents into the failed-documents index.
     * Returns the list of IDs that FAILED to index (empty = all succeeded).
     */
    public List<String> bulkIndexFailures(String index, List<FailureEntry> entries) {
        List<String> failed = new ArrayList<>();
        if (entries.isEmpty()) return failed;

        List<BulkOperation> bulkOperations = new ArrayList<>();
        for (FailureEntry e : entries) {
            try {
                bulkOperations.add(BulkOperation.of(b -> b
                        .index(idx -> idx
                                .index(index)
                                .id(e.id)
                                .document(e.document))));
            } catch (Exception ex) {
                log.error("Failed to create bulk failure operation. id={}", e.id, ex);
                failed.add(e.id);
            }
        }

        if (bulkOperations.isEmpty()) return failed;

        // Retry logic with exponential backoff
        int attempt = 0;
        long backoffMs = initialRetryBackoffMs;

        while (attempt <= maxRetries) {
            try {
                final List<BulkOperation> currentBulkOperations = bulkOperations;
                BulkResponse response = client.bulk(b -> b.operations(currentBulkOperations));
                List<String> attemptFailed = new ArrayList<>();

                if (response.errors()) {
                    final int attemptFinal=attempt;
                    response.items().forEach(item -> {
                        if (item.error() != null) {
                            if (isTransientFailure(item.status())) {
                                log.warn("Transient failure on attempt {}/{}. index={}, id={}, reason={}",
                                        attemptFinal + 1, maxRetries + 1, index, item.id(), item.error().reason());
                                attemptFailed.add(item.id());
                            } else {
                                log.error("Permanent failure. index={}, id={}, reason={}",
                                        index, item.id(), item.error().reason());
                                failed.add(item.id());
                            }
                        }
                    });
                }

                if (attemptFailed.isEmpty()) {
                    log.debug("Bulk indexed {} failure docs into '{}'. failures={}", entries.size(), index, failed.size());
                    return failed;
                }

                bulkOperations = rebuildFailureBulkRequest(index, entries, attemptFailed);
                failed.addAll(attemptFailed);

                if (attempt < maxRetries) {
                    log.info("Retrying {} transiently failed failure documents after {}ms backoff",
                            attemptFailed.size(), backoffMs);
                    Thread.sleep(backoffMs);
                    backoffMs = Math.min(backoffMs * 2, 5000);
                }

                attempt++;

            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                log.error("Bulk failure indexing interrupted", ie);
                entries.forEach(en -> failed.add(en.id));
                return failed;
            } catch (Exception e) {
                if (isTransientException(e) && attempt < maxRetries) {
                    log.warn("Transient exception in failure indexing on attempt {}/{}. Will retry after {}ms: {}",
                            attempt + 1, maxRetries + 1, backoffMs, e.getMessage());
                    try {
                        Thread.sleep(backoffMs);
                        backoffMs = Math.min(backoffMs * 2, 5000);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        entries.forEach(en -> failed.add(en.id));
                        return failed;
                    }
                } else {
                    log.error("Bulk failure request failed permanently for index={}. Marking all {} as failed.",
                            index, entries.size(), e);
                    entries.forEach(en -> failed.add(en.id));
                    return failed;
                }
                attempt++;
            }
        }

        return failed;
    }

    public boolean indexAsAllStrings(String rawIndex, String id,
                                     Map<String, Object> originalDocument,
                                     Map<String, RepairResult.UnconvertibleField> unconvertibleFields) {
        try {
            Map<String, Object> rawDoc = toStringValueMap(originalDocument, unconvertibleFields);
            String json = mapper.writeValueAsString(rawDoc);
            client.index(i -> i
                    .index(rawIndex)
                    .id(id)
                    .withJson(new java.io.StringReader(json)));
            log.debug("Indexed raw-string document. index={}, id={}", rawIndex, id);
            return true;
        } catch (Exception e) {
            log.error("Failed to index raw-string document. index={}, id={}", rawIndex, id, e);
            return false;
        }
    }

    // -------------------------------------------------------------------------
    // Bulk methods (used for main poll loop — high volume)
    // -------------------------------------------------------------------------

    /**
     * Bulk-indexes a list of repaired documents into the target index with retry logic.
     * Returns a map of record keys that FAILED to index with their error reasons (empty = all succeeded).
     */
    public Map<String, String> bulkIndex(String index, List<BulkEntry> entries) {
        Map<String, String> failed = new HashMap<>();
        if (entries.isEmpty()) return failed;

        List<BulkOperation> bulkOperations = new ArrayList<>();
        for (BulkEntry e : entries) {
            try {
                bulkOperations.add(BulkOperation.of(b -> b
                        .index(idx -> idx
                                .index(index)
                                .id(e.id)
                                .document(e.document))));
            } catch (Exception ex) {
                log.error("Failed to create bulk operation. id={}", e.id, ex);
                failed.put(e.id, "Failed to create bulk operation: " + ex.getMessage());
            }
        }

        if (bulkOperations.isEmpty()) return failed;

        // Check circuit breaker before attempting ES call
        if (circuitBreaker.getState() == CircuitBreaker.State.OPEN) {
            log.error("Circuit breaker is OPEN, marking all {} documents as failed", entries.size());
            entries.forEach(en -> failed.put(en.id, "Circuit breaker is OPEN"));
            return failed;
        }

        // Retry logic with exponential backoff
        int attempt = 0;
        long backoffMs = initialRetryBackoffMs;

        while (attempt <= maxRetries) {
            try {
                final List<BulkOperation> currentBulkOperations = bulkOperations;
                BulkResponse response = client.bulk(b -> b.operations(currentBulkOperations));
                List<String> attemptFailed = new ArrayList<>();

                if (response.errors()) {
                    final int attemptFinal=attempt;
                    final boolean[] fieldLimitErrorDetected = {false};
                    response.items().forEach(item -> {
                        if (item.error() != null) {
                            String reason = item.error().reason();
                            if (isFieldLimitError(reason)) {
                                // Field limit error - increase limit and retry
                                fieldLimitErrorDetected[0] = true;
                                attemptFailed.add(item.id());
                                log.warn("Field limit error detected for index={}, id={}. Will increase limit and retry.",
                                        index, item.id());
                            } else if (isTransientFailure(item.status())) {
                                // Transient failure - will retry
                                log.warn("Transient failure on attempt {}/{}. index={}, id={}, reason={}",
                                        attemptFinal + 1, maxRetries + 1, index, item.id(), reason);
                                attemptFailed.add(item.id());
                            } else {
                                // Permanent failure - no point retrying
                                log.error("Permanent failure. index={}, id={}, reason={}",
                                        index, item.id(), reason);
                                failed.put(item.id(), reason);
                            }
                        }
                    });
                    
                    // If field limit error detected, increase the limit
                    if (fieldLimitErrorDetected[0]) {
                        increaseFieldLimit(index);
                    }
                }

                if (attemptFailed.isEmpty()) {
                    // All succeeded or only permanent failures
                    log.debug("Bulk indexed {} docs into '{}'. failures={}", entries.size(), index, failed.size());
                    circuitBreaker.onSuccess(); // Notify circuit breaker of success
                    return failed;
                }

                // Rebuild bulk request with only transiently failed items
                bulkOperations = rebuildBulkRequest(index, entries, attemptFailed);
                failed.forEach((id, reason) -> attemptFailed.remove(id)); // Don't retry permanent failures

                if (attempt < maxRetries) {
                    log.info("Retrying {} transiently failed documents after {}ms backoff",
                            attemptFailed.size(), backoffMs);
                    Thread.sleep(backoffMs);
                    backoffMs = Math.min(backoffMs * 2, 5000); // Cap at 5 seconds
                }

                attempt++;

            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                log.error("Bulk indexing interrupted", ie);
                circuitBreaker.onFailure(); // Notify circuit breaker of failure
                entries.forEach(en -> failed.put(en.id, "Bulk indexing interrupted"));
                return failed;
            } catch (Exception e) {
                circuitBreaker.onFailure(); // Notify circuit breaker of failure
                if (isTransientException(e) && attempt < maxRetries) {
                    log.warn("Transient exception on attempt {}/{}. Will retry after {}ms: {}",
                            attempt + 1, maxRetries + 1, backoffMs, e.getMessage());
                    try {
                        Thread.sleep(backoffMs);
                        backoffMs = Math.min(backoffMs * 2, 5000);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        entries.forEach(en -> failed.put(en.id, "Bulk indexing interrupted"));
                        return failed;
                    }
                } else {
                    log.error("Bulk request failed permanently for index={}. Marking all {} as failed.",
                            index, entries.size(), e);
                    entries.forEach(en -> failed.put(en.id, "Bulk request failed permanently: " + e.getMessage()));
                    return failed;
                }
                attempt++;
            }
        }

        return failed;
    }

    /**
     * Bulk-indexes a list of documents into the raw fallback index with ALL field values as strings.
     * Failed IDs are returned (empty = all succeeded). Includes retry logic.
     */
    public List<String> bulkIndexAsAllStrings(String rawIndex, List<RawBulkEntry> entries) {
        List<String> failed = new ArrayList<>();
        if (entries.isEmpty()) return failed;

        List<BulkOperation> bulkOperations = new ArrayList<>();
        for (RawBulkEntry e : entries) {
            try {
                Map<String, Object> rawDoc = toStringValueMap(e.originalDocument, e.unconvertibleFields);
                String json = mapper.writeValueAsString(rawDoc);
                bulkOperations.add(BulkOperation.of(b -> b
                        .index(idx -> idx
                                .index(rawIndex)
                                .id(e.id)
                                .withJson(new java.io.StringReader(json)))));
            } catch (Exception ex) {
                log.error("Failed to serialize raw document for bulk. id={}", e.id, ex);
                failed.add(e.id);
            }
        }
        if (bulkOperations.isEmpty()) return failed;

        // Retry logic with exponential backoff
        int attempt = 0;
        long backoffMs = initialRetryBackoffMs;

        while (attempt <= maxRetries) {
            try {
                final List<BulkOperation> currentBulkOperations = bulkOperations;
                BulkResponse response = client.bulk(b -> b.operations(currentBulkOperations));
                List<String> attemptFailed = new ArrayList<>();
                final int attemptFinal=attempt;

                if (response.errors()) {
                    response.items().forEach(item -> {
                        if (item.error() != null) {
                            if (isTransientFailure(item.status())) {
                                log.warn("Transient raw-index failure on attempt {}/{}. index={}, id={}, reason={}",
                                        attemptFinal + 1, maxRetries + 1, rawIndex, item.id(), item.error().reason());
                                attemptFailed.add(item.id());
                            } else {
                                log.error("Permanent raw-index failure. index={}, id={}, reason={}",
                                        rawIndex, item.id(), item.error().reason());
                                failed.add(item.id());
                            }
                        }
                    });
                }

                if (attemptFailed.isEmpty()) {
                    log.debug("Bulk indexed {} raw docs into '{}'. failures={}", entries.size(), rawIndex, failed.size());
                    return failed;
                }

                bulkOperations = rebuildRawBulkRequest(rawIndex, entries, attemptFailed);
                failed.addAll(attemptFailed);

                if (attempt < maxRetries) {
                    log.info("Retrying {} transiently failed raw documents after {}ms backoff",
                            attemptFailed.size(), backoffMs);
                    Thread.sleep(backoffMs);
                    backoffMs = Math.min(backoffMs * 2, 5000);
                }

                attempt++;

            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                log.error("Bulk raw indexing interrupted", ie);
                entries.forEach(en -> failed.add(en.id));
                return failed;
            } catch (Exception e) {
                if (isTransientException(e) && attempt < maxRetries) {
                    log.warn("Transient exception in raw indexing on attempt {}/{}. Will retry after {}ms: {}",
                            attempt + 1, maxRetries + 1, backoffMs, e.getMessage());
                    try {
                        Thread.sleep(backoffMs);
                        backoffMs = Math.min(backoffMs * 2, 5000);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        entries.forEach(en -> failed.add(en.id));
                        return failed;
                    }
                } else {
                    log.error("Bulk raw request failed permanently for index={}. Marking all {} as failed.",
                            rawIndex, entries.size(), e);
                    entries.forEach(en -> failed.add(en.id));
                    return failed;
                }
                attempt++;
            }
        }

        return failed;
    }

    // -------------------------------------------------------------------------
    // Entry types for bulk operations
    // -------------------------------------------------------------------------

    public static class BulkEntry {
        public final String id;
        public final Map<String, Object> document;
        public BulkEntry(String id, Map<String, Object> document) {
            this.id = id;
            this.document = document;
        }
    }

    public static class RawBulkEntry {
        public final String id;
        public final Map<String, Object> originalDocument;
        public final Map<String, RepairResult.UnconvertibleField> unconvertibleFields;
        public RawBulkEntry(String id, Map<String, Object> originalDocument,
                            Map<String, RepairResult.UnconvertibleField> unconvertibleFields) {
            this.id = id;
            this.originalDocument = originalDocument;
            this.unconvertibleFields = unconvertibleFields;
        }
    }

    public static class FailureEntry {
        public final String id;
        public final Map<String, Object> document;
        public FailureEntry(String id, Map<String, Object> document) {
            this.id = id;
            this.document = document;
        }
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    /** Builds a Map where every value is a String, plus a _dlq_failures metadata field. */
    private Map<String, Object> toStringValueMap(Map<String, Object> original,
                                                  Map<String, RepairResult.UnconvertibleField> unconvertible) {
        Map<String, Object> result = new LinkedHashMap<>();

        // All original fields as strings
        for (Map.Entry<String, Object> e : original.entrySet()) {
            Object v = e.getValue();
            result.put(e.getKey(), v == null ? null : toStr(v));
        }

        // Overlay unconvertible fields with their original string values
        for (Map.Entry<String, RepairResult.UnconvertibleField> e : unconvertible.entrySet()) {
            result.put(e.getKey(), e.getValue().getOriginalValue());
        }

        // Embed failure reasons as a nested metadata field so they are queryable in ES
        // Structure: { "_dlq_failures": { "fieldName": { "esType": "...", "reason": "..." } } }
        if (!unconvertible.isEmpty()) {
            Map<String, Object> failures = new LinkedHashMap<>();
            for (Map.Entry<String, RepairResult.UnconvertibleField> e : unconvertible.entrySet()) {
                Map<String, String> detail = new LinkedHashMap<>();
                detail.put("esType", e.getValue().getEsType());
                detail.put("reason", e.getValue().getReason());
                failures.put(e.getKey(), detail);
            }
            result.put("_dlq_failures", failures);
        }

        return result;
    }

    private String toStr(Object value) {
        try {
            return (value instanceof String) ? (String) value : mapper.writeValueAsString(value);
        } catch (Exception e) {
            return String.valueOf(value);
        }
    }

    // -------------------------------------------------------------------------
    // Retry and Error Classification Helpers
    // -------------------------------------------------------------------------

    /**
     * Determines if an HTTP status code represents a transient failure.
     * Transient failures can be retried, permanent failures should not.
     */
    private boolean isTransientFailure(Integer status) {
        if (status == null) return false;
        return status == 429 || // Too Many Requests (rate limiting)
               status == 503 || // Service Unavailable
               status == 502 || // Bad Gateway
               status == 504;   // Gateway Timeout
    }

    /**
     * Determines if an error is a field limit error that can be fixed by increasing the limit.
     */
    private boolean isFieldLimitError(String reason) {
        if (reason == null) return false;
        return reason.contains("Limit of total fields") || 
               reason.toLowerCase().contains("total fields") ||
               reason.toLowerCase().contains("field limit");
    }

    /**
     * Automatically increases the field limit for an index when field limit errors occur.
     * Increases by 10000 each time to handle diverse document structures.
     */
    private void increaseFieldLimit(String index) {
        try {
            // Get current settings to check existing limit
            var currentSettings = client.indices().getSettings(s -> s.index(index));
            var currentLimitObj = currentSettings.result().get(index).settings().index().mapping().totalFields().limit();
            
            // Parse current limit as long (ES returns it as String)
            long currentLimit = 1000; // default
            if (currentLimitObj != null) {
                try {
                    currentLimit = Long.parseLong(currentLimitObj.toString());
                } catch (NumberFormatException e) {
                    log.warn("Could not parse current field limit '{}', using default 1000", currentLimitObj);
                }
            }
            
            // Increase by 10000
            long newLimit = currentLimit + 10000;
            
            log.warn("Increasing field limit for index '{}' from {} to {}", index, currentLimit, newLimit);
            
            client.indices().putSettings(s -> s
                    .index(index)
                    .withJson(new java.io.StringReader(
                            "{\"index.mapping.total_fields.limit\": " + newLimit + "}")));
            
            log.info("Successfully increased field limit for index '{}' to {}", index, newLimit);
            
        } catch (Exception e) {
            log.error("Failed to increase field limit for index '{}'", index, e);
        }
    }

    /**
     * Determines if an exception represents a transient failure.
     */
    private boolean isTransientException(Exception e) {
        // Network-related exceptions are typically transient
        String message = e.getMessage().toLowerCase();
        return message != null && (message.contains("connection") ||
               message.contains("timeout") ||
               message.contains("network") ||
               message.contains("unavailable"));
    }

    /**
     * Rebuilds a bulk request with only the failed entries for retry.
     */
    private List<BulkOperation> rebuildBulkRequest(String index, List<BulkEntry> allEntries, List<String> failedIds) {
        List<BulkOperation> newBulk = new ArrayList<>();
        for (BulkEntry entry : allEntries) {
            if (failedIds.contains(entry.id)) {
                try {
                    newBulk.add(BulkOperation.of(b -> b
                            .index(idx -> idx
                                    .index(index)
                                    .id(entry.id)
                                    .document(entry.document))));
                } catch (Exception ex) {
                    log.error("Failed to rebuild bulk request for id={}", entry.id, ex);
                }
            }
        }
        return newBulk;
    }

    /**
     * Rebuilds a raw bulk request with only the failed entries for retry.
     */
    private List<BulkOperation> rebuildRawBulkRequest(String index, List<RawBulkEntry> allEntries, List<String> failedIds) {
        List<BulkOperation> newBulk = new ArrayList<>();
        for (RawBulkEntry entry : allEntries) {
            if (failedIds.contains(entry.id)) {
                try {
                    Map<String, Object> rawDoc = toStringValueMap(entry.originalDocument, entry.unconvertibleFields);
                    newBulk.add(BulkOperation.of(b -> b
                            .index(idx -> idx
                                    .index(index)
                                    .id(entry.id)
                                    .document(rawDoc))));
                } catch (Exception ex) {
                    log.error("Failed to rebuild raw bulk request for id={}", entry.id, ex);
                }
            }
        }
        return newBulk;
    }

    /**
     * Rebuilds a failure bulk request with only the failed entries for retry.
     */
    private List<BulkOperation> rebuildFailureBulkRequest(String index, List<FailureEntry> allEntries, List<String> failedIds) {
        List<BulkOperation> newBulk = new ArrayList<>();
        for (FailureEntry entry : allEntries) {
            if (failedIds.contains(entry.id)) {
                try {
                    newBulk.add(BulkOperation.of(b -> b
                            .index(idx -> idx
                                    .index(index)
                                    .id(entry.id)
                                    .document(entry.document))));
                } catch (Exception ex) {
                    log.error("Failed to rebuild failure bulk request for id={}", entry.id, ex);
                }
            }
        }
        return newBulk;
    }
}
