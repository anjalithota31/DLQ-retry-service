package com.tyss.dlq;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.indices.GetMappingResponse;
import co.elastic.clients.elasticsearch.indices.get_mapping.IndexMappingRecord;
import co.elastic.clients.elasticsearch._types.mapping.TypeMapping;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Caching layer for Elasticsearch index mappings.
 * Dynamically loads and caches mappings for multiple indices.
 */
public class MappingCache {

    private static final Logger log = LoggerFactory.getLogger(MappingCache.class);

    private final ElasticsearchClient esClient;
    private final Cache<String, Map<String, Object>> cache;

    /**
     * Creates a new mapping cache with the specified configuration.
     *
     * @param esClient Elasticsearch client
     * @param maxSize Maximum number of mappings to cache
     * @param ttlMinutes Time-to-live for cached mappings in minutes
     */
    public MappingCache(ElasticsearchClient esClient, int maxSize, long ttlMinutes) {
        this.esClient = esClient;
        this.cache = Caffeine.newBuilder()
                .maximumSize(maxSize)
                .expireAfterWrite(ttlMinutes, TimeUnit.MINUTES)
                .refreshAfterWrite(ttlMinutes / 2, TimeUnit.MINUTES)
                .build(this::loadMapping);
        log.info("MappingCache initialized with maxSize={}, ttlMinutes={}", maxSize, ttlMinutes);
    }

    /**
     * Gets the mapping for the specified index.
     * Returns cached mapping if available, otherwise loads from Elasticsearch.
     *
     * @param index The Elasticsearch index name
     * @return Map of field names to their mapping definitions
     */
    public Map<String, Object> getMapping(String index) {
        try {
            return cache.get(index, this::loadMapping);
        } catch (Exception e) {
            log.warn("Failed to get mapping for index '{}', returning empty map", index, e);
            return Collections.emptyMap();
        }
    }

    /**
     * Loads the mapping for the specified index from Elasticsearch.
     * This is called automatically by the cache when a mapping is not cached.
     *
     * @param index The Elasticsearch index name
     * @return Map of field names to their mapping definitions
     */
    private Map<String, Object> loadMapping(String index) {
        try {
            log.info("Attempting to load mapping for index: '{}'", index);
            GetMappingResponse response = esClient.indices()
                    .getMapping(m -> m.index(index));

            IndexMappingRecord mappingRecord = response.result().get(index);
            if (mappingRecord == null || mappingRecord.mappings() == null) {
                log.warn("No mapping found for index '{}'. The index may not exist. Repair will skip field validation.", index);
                log.warn("Available indices in response: {}", response.result().keySet());
                return Collections.emptyMap();
            }

            TypeMapping mapping = mappingRecord.mappings();
            if (mapping.properties() == null) {
                log.warn("Mapping found for index '{}' but has no properties. Repair will skip field validation.", index);
                return Collections.emptyMap();
            }

            // Convert Property objects to Map<String, Object> for compatibility
            Map<String, Object> properties = new java.util.HashMap<>();
            mapping.properties().forEach((fieldName, property) -> {
                properties.put(fieldName, convertPropertyToMap(property));
            });

            log.info("Successfully loaded ES mapping for index '{}': {} fields", index, properties.size());
            return properties;

        } catch (Exception e) {
            log.error("Failed to load mapping for index '{}'. Error: {}", index, e.getMessage(), e);
            return Collections.emptyMap();
        }
    }

    /**
     * Converts a Property object to a Map representation for compatibility with DocumentRepairer.
     * Recursively extracts nested properties to support fields like "defectDetails.Assign to".
     */
    private Map<String, Object> convertPropertyToMap(co.elastic.clients.elasticsearch._types.mapping.Property property) {
        Map<String, Object> result = new java.util.HashMap<>();
        if (property._kind() != null) {
            result.put("type", property._kind().jsonValue());
        }
        
        // Extract nested properties for object/nested types
        // Use the internal API to access properties for object types
        try {
            String kind = property._kind() != null ? property._kind().jsonValue() : "";
            if ("object".equals(kind) || "nested".equals(kind)) {
                // Access properties through reflection
                java.lang.reflect.Field propertiesField = property.getClass().getDeclaredField("properties");
                propertiesField.setAccessible(true);
                @SuppressWarnings("unchecked")
                java.util.Map<String, co.elastic.clients.elasticsearch._types.mapping.Property> nestedPropsMap = 
                    (java.util.Map<String, co.elastic.clients.elasticsearch._types.mapping.Property>) propertiesField.get(property);
                
                if (nestedPropsMap != null && !nestedPropsMap.isEmpty()) {
                    Map<String, Object> nestedProps = new java.util.HashMap<>();
                    nestedPropsMap.forEach((nestedFieldName, nestedProperty) -> {
                        nestedProps.put(nestedFieldName, convertPropertyToMap(nestedProperty));
                    });
                    result.put("properties", nestedProps);
                }
            }
        } catch (Exception e) {
            log.debug("Could not extract nested properties for field: {}", e.getMessage());
        }
        
        return result;
    }

    /**
     * Invalidates the cached mapping for the specified index.
     * Useful when you know the mapping has changed and want to force a reload.
     *
     * @param index The Elasticsearch index name
     */
    public void invalidate(String index) {
        cache.invalidate(index);
        log.info("Invalidated cache for index '{}'", index);
    }

    /**
     * Invalidates all cached mappings.
     */
    public void invalidateAll() {
        cache.invalidateAll();
        log.info("Invalidated all cached mappings");
    }

    /**
     * Returns cache statistics for monitoring.
     *
     * @return Cache statistics as a string
     */
    public String getStats() {
        com.github.benmanes.caffeine.cache.stats.CacheStats stats = cache.stats();
        return String.format("CacheStats{hitRate=%.2f%%, hitCount=%d, missCount=%d, loadSuccessCount=%d, loadFailureCount=%d, totalLoadTime=%dms}",
                stats.hitRate() * 100,
                stats.hitCount(),
                stats.missCount(),
                stats.loadSuccessCount(),
                stats.loadFailureCount(),
                stats.totalLoadTime() / 1_000_000);
    }
}
