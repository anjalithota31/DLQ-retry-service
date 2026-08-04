package com.tyss.dlq;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongArray;

/**
 * Simple metrics collector for monitoring DLQ repair service performance.
 * Thread-safe metrics for production monitoring.
 */
public class MetricsCollector {
    
    private static final Logger log = LoggerFactory.getLogger(MetricsCollector.class);
    
    // Counter metrics
    private final AtomicLong recordsProcessed = new AtomicLong(0);
    private final AtomicLong recordsSucceeded = new AtomicLong(0);
    private final AtomicLong recordsFailed = new AtomicLong(0);
    private final AtomicLong recordsRepaired = new AtomicLong(0);
    private final AtomicLong recordsWithUnconvertibleFields = new AtomicLong(0);
    
    // Timing metrics (in milliseconds)
    private final AtomicLong totalProcessingTimeMs = new AtomicLong(0);
    private final AtomicLong minProcessingTimeMs = new AtomicLong(Long.MAX_VALUE);
    private final AtomicLong maxProcessingTimeMs = new AtomicLong(0);
    
    // Error classification metrics
    private final AtomicLong transientFailures = new AtomicLong(0);
    private final AtomicLong permanentFailures = new AtomicLong(0);
    
    // Elasticsearch operation metrics
    private final AtomicLong esBulkOperations = new AtomicLong(0);
    private final AtomicLong esBulkRetries = new AtomicLong(0);
    
    // Circuit breaker metrics
    private final AtomicLong circuitBreakerOpens = new AtomicLong(0);
    private final AtomicLong circuitBreakerCloses = new AtomicLong(0);

    // Failed-documents index metrics
    private final AtomicLong failedDocsIndexFailures = new AtomicLong(0);
    
    public void incrementRecordsProcessed() {
        recordsProcessed.incrementAndGet();
    }
    
    public void incrementRecordsSucceeded() {
        recordsSucceeded.incrementAndGet();
    }
    
    public void incrementRecordsSucceeded(long count) {
        recordsSucceeded.addAndGet(count);
    }
    
    public void incrementRecordsFailed() {
        recordsFailed.incrementAndGet();
    }
    
    public void incrementRecordsFailed(long count) {
        recordsFailed.addAndGet(count);
    }
    
    public void incrementRecordsRepaired() {
        recordsRepaired.incrementAndGet();
    }
    
    public void incrementRecordsWithUnconvertibleFields() {
        recordsWithUnconvertibleFields.incrementAndGet();
    }
    
    public void recordProcessingTime(long timeMs) {
        totalProcessingTimeMs.addAndGet(timeMs);
        
        // Update min (use compareAndSet for thread safety)
        long currentMin = minProcessingTimeMs.get();
        while (timeMs < currentMin && !minProcessingTimeMs.compareAndSet(currentMin, timeMs)) {
            currentMin = minProcessingTimeMs.get();
        }
        
        // Update max
        long currentMax = maxProcessingTimeMs.get();
        while (timeMs > currentMax && !maxProcessingTimeMs.compareAndSet(currentMax, timeMs)) {
            currentMax = maxProcessingTimeMs.get();
        }
    }
    
    public void incrementTransientFailures() {
        transientFailures.incrementAndGet();
    }
    
    public void incrementPermanentFailures() {
        permanentFailures.incrementAndGet();
    }
    
    public void incrementEsBulkOperations() {
        esBulkOperations.incrementAndGet();
    }
    
    public void incrementEsBulkRetries() {
        esBulkRetries.incrementAndGet();
    }
    
    public void incrementCircuitBreakerOpens() {
        circuitBreakerOpens.incrementAndGet();
    }
    
    public void incrementCircuitBreakerCloses() {
        circuitBreakerCloses.incrementAndGet();
    }

    public void incrementFailedDocsIndexFailures() {
        failedDocsIndexFailures.incrementAndGet();
    }

    public void incrementFailedDocsIndexFailures(long count) {
        failedDocsIndexFailures.addAndGet(count);
    }
    
    public String getMetricsSummary() {
        long processed = recordsProcessed.get();
        long succeeded = recordsSucceeded.get();
        long failed = recordsFailed.get();
        long repaired = recordsRepaired.get();
        long unconvertible = recordsWithUnconvertibleFields.get();
        
        double successRate = processed > 0 ? (succeeded * 100.0 / processed) : 0.0;
        double avgProcessingTime = processed > 0 ? (totalProcessingTimeMs.get() * 1.0 / processed) : 0.0;
        
        return String.format(
            "Metrics: processed=%d, succeeded=%d (%.2f%%), failed=%d, repaired=%d, unconvertible=%d, " +
            "avgProcessingTime=%.2fms, transientFailures=%d, permanentFailures=%d, " +
            "esBulkOps=%d, esRetries=%d, circuitBreakerOpens=%d, circuitBreakerCloses=%d, failedDocsIndexFailures=%d",
            processed, succeeded, successRate, failed, repaired, unconvertible,
            avgProcessingTime, transientFailures.get(), permanentFailures.get(),
            esBulkOperations.get(), esBulkRetries.get(), circuitBreakerOpens.get(), circuitBreakerCloses.get(),
            failedDocsIndexFailures.get()
        );
    }
    
    public void logMetrics() {
        log.info(getMetricsSummary());
    }
    
    public void reset() {
        recordsProcessed.set(0);
        recordsSucceeded.set(0);
        recordsFailed.set(0);
        recordsRepaired.set(0);
        recordsWithUnconvertibleFields.set(0);
        totalProcessingTimeMs.set(0);
        minProcessingTimeMs.set(Long.MAX_VALUE);
        maxProcessingTimeMs.set(0);
        transientFailures.set(0);
        permanentFailures.set(0);
        esBulkOperations.set(0);
        esBulkRetries.set(0);
        circuitBreakerOpens.set(0);
        circuitBreakerCloses.set(0);
        failedDocsIndexFailures.set(0);
    }
}
