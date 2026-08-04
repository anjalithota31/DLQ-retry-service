package com.tyss.dlq;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Simple Circuit Breaker implementation for Elasticsearch calls.
 * 
 * States:
 * - CLOSED: Normal operation, requests pass through
 * - OPEN: Circuit is open, requests fail fast
 * - HALF_OPEN: Testing if service has recovered
 */
public class CircuitBreaker {
    
    private static final Logger log = LoggerFactory.getLogger(CircuitBreaker.class);
    
    private final int failureThreshold;
    private final int successThreshold;
    private final long timeoutMs;
    private final AtomicInteger failureCount = new AtomicInteger(0);
    private final AtomicInteger successCount = new AtomicInteger(0);
    private final AtomicLong lastFailureTime = new AtomicLong(0);
    private volatile State state = State.CLOSED;

    public enum State {
        CLOSED,    // Normal operation
        OPEN,      // Circuit is open, failing fast
        HALF_OPEN  // Testing if service recovered
    }

    public CircuitBreaker(int failureThreshold, long timeoutMs) {
        this(failureThreshold, 3, timeoutMs); // default: 3 successes to close from HALF_OPEN
    }

    public CircuitBreaker(int failureThreshold, int successThreshold, long timeoutMs) {
        this.failureThreshold = failureThreshold;
        this.successThreshold = successThreshold;
        this.timeoutMs = timeoutMs;
    }

    public CircuitBreaker() {
        this(5, 3, 60000); // default: 5 failures, 3 successes, 60 second timeout
    }
    
    /**
     * Executes the given operation with circuit breaker protection.
     * Returns true if operation succeeded, false if circuit is open or operation failed.
     */
    public boolean execute(CircuitBreakerOperation operation) {
        if (!allowRequest()) {
            log.warn("Circuit breaker is OPEN, rejecting request");
            return false;
        }
        
        try {
            boolean result = operation.execute();
            onSuccess();
            return result;
        } catch (Exception e) {
            onFailure();
            log.error("Operation failed with circuit breaker", e);
            return false;
        }
    }
    
    /**
     * Determines if a request should be allowed based on circuit breaker state.
     */
    private boolean allowRequest() {
        if (state == State.CLOSED) {
            return true;
        }
        
        if (state == State.OPEN) {
            long timeSinceLastFailure = System.currentTimeMillis() - lastFailureTime.get();
            if (timeSinceLastFailure > timeoutMs) {
                // Transition to HALF_OPEN to test if service recovered
                log.info("Circuit breaker transitioning from OPEN to HALF_OPEN");
                state = State.HALF_OPEN;
                return true;
            }
            return false;
        }
        
        // HALF_OPEN state - allow one request to test
        return true;
    }
    
    /**
     * Called when an operation succeeds.
     */
    public void onSuccess() {
        if (state == State.HALF_OPEN) {
            int successes = successCount.incrementAndGet();
            if (successes >= successThreshold) {
                log.info("Circuit breaker transitioning from HALF_OPEN to CLOSED after {} successes", successes);
                state = State.CLOSED;
                failureCount.set(0);
                successCount.set(0);
            } else {
                log.debug("Circuit breaker in HALF_OPEN state, success count: {}/{}", successes, successThreshold);
            }
        } else if (state == State.CLOSED) {
            failureCount.set(0);
            successCount.set(0);
        }
    }
    
    /**
     * Called when an operation fails.
     */
    public void onFailure() {
        int failures = failureCount.incrementAndGet();
        lastFailureTime.set(System.currentTimeMillis());

        if (failures >= failureThreshold) {
            if (state != State.OPEN) {
                log.warn("Circuit breaker transitioning to OPEN after {} failures", failures);
                state = State.OPEN;
                successCount.set(0); // Reset success count when opening
            }
        }
    }
    
    public State getState() {
        return state;
    }
    
    public int getFailureCount() {
        return failureCount.get();
    }
    
    public void reset() {
        log.info("Circuit breaker manually reset to CLOSED state");
        state = State.CLOSED;
        failureCount.set(0);
        lastFailureTime.set(0);
    }
    
    @FunctionalInterface
    public interface CircuitBreakerOperation {
        boolean execute() throws Exception;
    }
}
