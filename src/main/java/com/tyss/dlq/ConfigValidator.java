package com.tyss.dlq;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * Validates configuration properties for the DLQ Repair Service.
 * Ensures required properties are present and valid before startup.
 */
public class ConfigValidator {
    
    private static final Logger log = LoggerFactory.getLogger(ConfigValidator.class);
    
    public static void validate(Properties config) throws IllegalArgumentException {
        log.info("Validating configuration...");
        
        // Required properties
        // Either dlq.topic or dlq.topic.pattern must be specified
        String dlqTopic = config.getProperty("dlq.topic");
        String dlqTopicPattern = config.getProperty("dlq.topic.pattern");
        if ((dlqTopic == null || dlqTopic.trim().isEmpty()) && 
            (dlqTopicPattern == null || dlqTopicPattern.trim().isEmpty())) {
            throw new IllegalArgumentException("Either dlq.topic or dlq.topic.pattern must be specified");
        }
        requireNonEmpty(config, "elasticsearch.url", "Elasticsearch URL must be specified");
        requireNonEmpty(config, "kafka.bootstrap.servers", "Kafka bootstrap servers must be specified");
        requireNonEmpty(config, "corrected.docs.index", "Corrected documents index must be specified");
        
        // Numeric properties with validation
        requirePositiveInt(config, "max.retries", "Max retries must be positive");
        requirePositiveInt(config, "max.conversion.retries", "Max conversion retries must be positive");
        requirePositiveInt(config, "kafka.connection.max.retries", "Kafka connection max retries must be positive");
        requirePositiveLong(config, "retry.backoff.ms", "Retry backoff must be positive");
        requirePositiveLong(config, "kafka.connection.retry.backoff.ms", "Kafka connection retry backoff must be positive");
        requirePositiveInt(config, "circuit.breaker.failure.threshold", "Circuit breaker threshold must be positive");
        requirePositiveLong(config, "circuit.breaker.timeout.ms", "Circuit breaker timeout must be positive");
        requirePositiveInt(config, "consumer.max.poll.records", "Max poll records must be positive");
        requirePositiveLong(config, "consumer.max.poll.interval.ms", "Max poll interval must be positive");
        requirePositiveLong(config, "consumer.session.timeout.ms", "Session timeout must be positive");
        requirePositiveLong(config, "consumer.heartbeat.interval.ms", "Heartbeat interval must be positive");
        requirePositiveLong(config, "consumer.metadata.timeout.ms", "Metadata timeout must be positive");
        requirePositiveLong(config, "consumer.request.timeout.ms", "Request timeout must be positive");
        requirePositiveLong(config, "consumer.rebalance.timeout.ms", "Rebalance timeout must be positive");
        requirePositiveInt(config, "consumer.fetch.min.bytes", "Fetch min bytes must be positive");
        requirePositiveInt(config, "consumer.fetch.max.wait.ms", "Fetch max wait must be positive");
        requirePositiveLong(config, "mapping.refresh.interval.ms", "Mapping refresh interval must be positive");
        requireValidPort(config, "health.check.port", "Health check port must be valid (1-65535)");
        
        // Boolean properties
        requireValidBoolean(config, "elasticsearch.ssl.verify", "SSL verify must be true or false");
        
        // Optional but recommended properties
        if (config.getProperty("elasticsearch.url").startsWith("https://")) {
            if (config.getProperty("elasticsearch.username") == null || 
                config.getProperty("elasticsearch.username").isEmpty()) {
                log.warn("HTTPS URL specified but no username configured. Authentication may fail.");
            }
        }
        
        log.info("Configuration validation passed");
    }
    
    private static void requireNonEmpty(Properties config, String key, String errorMessage) {
        String value = config.getProperty(key);
        if (value == null || value.trim().isEmpty()) {
            throw new IllegalArgumentException(errorMessage + " (missing: " + key + ")");
        }
    }
    
    private static void requirePositiveInt(Properties config, String key, String errorMessage) {
        String value = config.getProperty(key);
        if (value == null || value.trim().isEmpty()) {
            return; // Use default
        }
        try {
            int intValue = Integer.parseInt(value);
            if (intValue <= 0) {
                throw new IllegalArgumentException(errorMessage + " (got: " + intValue + " for " + key + ")");
            }
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(errorMessage + " (invalid number for " + key + ": " + value + ")");
        }
    }
    
    private static void requirePositiveLong(Properties config, String key, String errorMessage) {
        String value = config.getProperty(key);
        if (value == null || value.trim().isEmpty()) {
            return; // Use default
        }
        try {
            long longValue = Long.parseLong(value);
            if (longValue <= 0) {
                throw new IllegalArgumentException(errorMessage + " (got: " + longValue + " for " + key + ")");
            }
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(errorMessage + " (invalid number for " + key + ": " + value + ")");
        }
    }
    
    private static void requireValidPort(Properties config, String key, String errorMessage) {
        String value = config.getProperty(key);
        if (value == null || value.trim().isEmpty()) {
            return; // Use default
        }
        try {
            int port = Integer.parseInt(value);
            if (port < 1 || port > 65535) {
                throw new IllegalArgumentException(errorMessage + " (got: " + port + " for " + key + ")");
            }
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(errorMessage + " (invalid port for " + key + ": " + value + ")");
        }
    }
    
    private static void requireValidBoolean(Properties config, String key, String errorMessage) {
        String value = config.getProperty(key);
        if (value == null || value.trim().isEmpty()) {
            return; // Use default
        }
        if (!"true".equalsIgnoreCase(value) && !"false".equalsIgnoreCase(value)) {
            throw new IllegalArgumentException(errorMessage + " (got: " + value + " for " + key + ")");
        }
    }
}
