package com.tyss.dlq;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Simple rate limiter to control processing rate.
 * Uses token bucket algorithm for smooth rate limiting.
 */
public class RateLimiter {
    
    private static final Logger log = LoggerFactory.getLogger(RateLimiter.class);
    
    private final long permitsPerSecond;
    private final AtomicLong availablePermits;
    private final long refillIntervalNanos;
    private long lastRefillTimestamp;
    
    /**
     * Creates a rate limiter with the specified permits per second.
     * @param permitsPerSecond maximum operations per second
     */
    public RateLimiter(double permitsPerSecond) {
        this.permitsPerSecond = (long) (permitsPerSecond * 1_000_000_000); // Convert to nanos
        this.availablePermits = new AtomicLong(this.permitsPerSecond);
        this.refillIntervalNanos = 1_000_000_000; // 1 second in nanos
        this.lastRefillTimestamp = System.nanoTime();
    }
    
    /**
     * Acquires a permit, blocking if necessary until one is available.
     */
    public void acquire() throws InterruptedException {
        acquire(1);
    }
    
    /**
     * Acquires the specified number of permits, blocking if necessary.
     * @param permits number of permits to acquire
     */
    public void acquire(int permits) throws InterruptedException {
        if (permits <= 0) {
            return;
        }
        
        while (true) {
            long now = System.nanoTime();
            refill(now);
            
            long current = availablePermits.get();
            if (current >= permits) {
                if (availablePermits.compareAndSet(current, current - permits)) {
                    return;
                }
            } else {
                // Calculate wait time
                long needed = permits - current;
                long waitNanos = (needed * refillIntervalNanos) / permitsPerSecond;
                long waitMs = Math.max(waitNanos / 1_000_000, 1); // Minimum 1ms sleep

                log.debug("Rate limiting: waiting {}ms for {} permits", waitMs, permits);
                Thread.sleep(waitMs);
            }
        }
    }
    
    /**
     * Tries to acquire a permit without blocking.
     * @return true if permit was acquired, false otherwise
     */
    public boolean tryAcquire() {
        return tryAcquire(1);
    }
    
    /**
     * Tries to acquire the specified permits without blocking.
     * @param permits number of permits to acquire
     * @return true if permits were acquired, false otherwise
     */
    public boolean tryAcquire(int permits) {
        if (permits <= 0) {
            return true;
        }
        
        refill(System.nanoTime());
        
        long current = availablePermits.get();
        if (current >= permits) {
            return availablePermits.compareAndSet(current, current - permits);
        }
        return false;
    }
    
    /**
     * Refills the token bucket based on elapsed time.
     */
    private void refill(long now) {
        long elapsed = now - lastRefillTimestamp;
        if (elapsed >= refillIntervalNanos) {
            long newPermits = (elapsed * permitsPerSecond) / refillIntervalNanos;
            long current = availablePermits.get();
            long newValue = Math.min(current + newPermits, permitsPerSecond);
            
            if (availablePermits.compareAndSet(current, newValue)) {
                lastRefillTimestamp = now;
            }
        }
    }
    
    /**
     * Gets the current available permits.
     */
    public double getAvailablePermits() {
        return availablePermits.get() / 1_000_000_000.0;
    }
}
