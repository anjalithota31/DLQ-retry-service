package com.tyss.dlq;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.time.*;
import java.time.format.*;

/**
 * Core repair engine.
 *
 * For each field in the document:
 *   1. If the field exists in the ES mapping and the value is valid → keep as-is
 *   2. If the value is wrong type but repairable → apply repair strategy
 *   3. If the value cannot be repaired → remove the field
 *
 * Fields not present in the mapping are passed through unchanged
 * (ES dynamic mapping will handle them, or they will be ignored).
 */
public class DocumentRepairer {

    private static final Logger log = LoggerFactory.getLogger(DocumentRepairer.class);

    private final Map<String, Object> indexMapping;  // ES "properties" map
    private final ObjectMapper mapper;

    public DocumentRepairer(Map<String, Object> indexMapping, ObjectMapper mapper) {
        this.indexMapping = indexMapping;
        this.mapper = mapper;
    }

    /**
     * Analyzes and repairs the document in a single pass.
     * Returns a RepairResult containing the cleaned document plus audit history.
     * Handles nested field paths (e.g., "defectDetails.Assign to").
     */
    @SuppressWarnings("unchecked")
    public RepairResult repair(Map<String, Object> document) {
        Map<String, Object>             repairedDoc         = new LinkedHashMap<>(document);
        List<RepairAction>               repairedFields      = new ArrayList<>();
        List<RemovedField>               removedFields       = new ArrayList<>();
        Map<String, RepairResult.UnconvertibleField> unconvertibleFields = new LinkedHashMap<>();

        // First, repair top-level fields
        for (Map.Entry<String, Object> entry : document.entrySet()) {
            String fieldName  = entry.getKey();
            Object fieldValue = entry.getValue();

            if (!indexMapping.containsKey(fieldName)) {
                continue; // not in mapping — pass through, ES dynamic mapping handles it
            }

            Map<String, Object> fieldMapping = (Map<String, Object>) indexMapping.get(fieldName);
            String esType = (String) fieldMapping.getOrDefault("type", "object");

            if (fieldValue == null) continue; // null is valid for any ES type

            RepairOutcome outcome = repairField(fieldName, fieldValue, esType);

            switch (outcome.getStatus()) {
                case VALID:
                    break;
                case REPAIRED:
                    repairedDoc.put(fieldName, outcome.getRepairedValue());
                    repairedFields.add(new RepairAction(fieldName, esType,
                            String.valueOf(fieldValue), String.valueOf(outcome.getRepairedValue()),
                            outcome.getStrategy()));
                    log.debug("Repaired field '{}': '{}' → '{}' (strategy={})",
                            fieldName, fieldValue, outcome.getRepairedValue(), outcome.getStrategy());
                    break;
                case REMOVED:
                    repairedDoc.remove(fieldName);
                    removedFields.add(new RemovedField(fieldName, esType,
                            String.valueOf(fieldValue), outcome.getReason()));
                    log.warn("Removed field '{}': value='{}', reason='{}'",
                            fieldName, fieldValue, outcome.getReason());
                    break;
                case UNCONVERTIBLE:
                    repairedDoc.remove(fieldName);
                    unconvertibleFields.put(fieldName, new RepairResult.UnconvertibleField(
                            toRawString(fieldValue), esType, outcome.getReason()));
                    log.warn("Unconvertible field '{}' (esType={}): kept as string in raw index. reason='{}'",
                            fieldName, esType, outcome.getReason());
                    break;
            }
        }

        // Then, repair nested fields within objects
        repairNestedFields(repairedDoc, indexMapping, "", repairedFields, removedFields, unconvertibleFields);

        return new RepairResult(repairedDoc, repairedFields, removedFields, unconvertibleFields);
    }

    /**
     * Recursively repairs nested fields within object structures.
     * Handles paths like "defectDetails.Assign to" where nested fields may have type mismatches.
     */
    @SuppressWarnings("unchecked")
    private void repairNestedFields(Map<String, Object> document, Map<String, Object> mapping, String pathPrefix,
                                    List<RepairAction> repairedFields, List<RemovedField> removedFields,
                                    Map<String, RepairResult.UnconvertibleField> unconvertibleFields) {
        for (Map.Entry<String, Object> entry : document.entrySet()) {
            String fieldName = entry.getKey();
            Object fieldValue = entry.getValue();
            String fullPath = pathPrefix.isEmpty() ? fieldName : pathPrefix + "." + fieldName;

            if (fieldValue == null || !(fieldValue instanceof Map)) {
                continue; // Skip null values and non-object values
            }

            Map<String, Object> fieldValueMap = (Map<String, Object>) fieldValue;
            Map<String, Object> fieldMapping = (Map<String, Object>) mapping.get(fieldName);

            if (fieldMapping == null) {
                continue; // No mapping for this field, skip nested repair
            }

            String esType = (String) fieldMapping.getOrDefault("type", "object");
            Map<String, Object> nestedProperties = (Map<String, Object>) fieldMapping.get("properties");

            // If this field has nested properties, repair them
            if (nestedProperties != null && !nestedProperties.isEmpty()) {
                for (Map.Entry<String, Object> nestedEntry : fieldValueMap.entrySet()) {
                    String nestedFieldName = nestedEntry.getKey();
                    Object nestedFieldValue = nestedEntry.getValue();
                    String nestedFullPath = fullPath + "." + nestedFieldName;

                    if (!nestedProperties.containsKey(nestedFieldName)) {
                        continue; // Nested field not in mapping, skip
                    }

                    Map<String, Object> nestedFieldMapping = (Map<String, Object>) nestedProperties.get(nestedFieldName);
                    String nestedEsType = (String) nestedFieldMapping.getOrDefault("type", "object");

                    if (nestedFieldValue == null) continue; // null is valid for any ES type

                    RepairOutcome outcome = repairField(nestedFullPath, nestedFieldValue, nestedEsType);

                    switch (outcome.getStatus()) {
                        case VALID:
                            break;
                        case REPAIRED:
                            fieldValueMap.put(nestedFieldName, outcome.getRepairedValue());
                            repairedFields.add(new RepairAction(nestedFullPath, nestedEsType,
                                    String.valueOf(nestedFieldValue), String.valueOf(outcome.getRepairedValue()),
                                    outcome.getStrategy()));
                            log.debug("Repaired nested field '{}': '{}' → '{}' (strategy={})",
                                    nestedFullPath, nestedFieldValue, outcome.getRepairedValue(), outcome.getStrategy());
                            break;
                        case REMOVED:
                            fieldValueMap.remove(nestedFieldName);
                            removedFields.add(new RemovedField(nestedFullPath, nestedEsType,
                                    String.valueOf(nestedFieldValue), outcome.getReason()));
                            log.warn("Removed nested field '{}': value='{}', reason='{}'",
                                    nestedFullPath, nestedFieldValue, outcome.getReason());
                            break;
                        case UNCONVERTIBLE:
                            fieldValueMap.remove(nestedFieldName);
                            unconvertibleFields.put(nestedFullPath, new RepairResult.UnconvertibleField(
                                    toRawString(nestedFieldValue), nestedEsType, outcome.getReason()));
                            log.warn("Unconvertible nested field '{}' (esType={}): kept as string in raw index. reason='{}'",
                                    nestedFullPath, nestedEsType, outcome.getReason());
                            break;
                    }
                }

                // Recursively repair deeper nested structures
                repairNestedFields(fieldValueMap, nestedProperties, fullPath, repairedFields, removedFields, unconvertibleFields);
            }
        }
    }

    /** Converts any value to its string representation for the raw fallback index. */
    private String toRawString(Object value) {
        try {
            return (value instanceof String) ? (String) value : mapper.writeValueAsString(value);
        } catch (Exception e) {
            return String.valueOf(value);
        }
    }

    /**
     * Attempts to repair a single field value to match the expected ES type.
     */
    private RepairOutcome repairField(String fieldName, Object value, String esType) {
        try {
            switch (esType) {
                case "date":      return repairDate(value);
                case "long":
                case "integer":
                case "short":
                case "byte":      return repairInteger(value, esType);
                case "double":
                case "float":
                case "half_float":
                case "scaled_float": return repairDecimal(value, esType);
                case "boolean":   return repairBoolean(value);
                case "keyword":
                case "text":      return repairText(value, esType);
                default:
                    // object, nested, geo_point, etc. — pass through
                    return RepairOutcome.valid();
            }
        } catch (Exception e) {
            return RepairOutcome.removed("Unexpected error during repair: " + e.getMessage());
        }
    }

    // -------------------------------------------------------------------------
    // Repair strategies per ES type
    // -------------------------------------------------------------------------

    /**
     * Date repair:
     *  - String: try common date formats; if parseable → keep as ISO-8601 string
     *  - Number: epoch millis → valid
     *  - Anything else → UNCONVERTIBLE (saved as string in raw index)
     */
    private RepairOutcome repairDate(Object value) {
        if (value instanceof Number) return RepairOutcome.valid(); // epoch millis
        if (value instanceof String) {
            String s = ((String) value).trim();
            if (s.isEmpty()) return RepairOutcome.unconvertible("Empty string cannot be stored as date");
            // Try ISO-8601 first (ES native)
            try { Instant.parse(s); return RepairOutcome.valid(); } catch (DateTimeParseException ignored) {}
            // Try common date-only format yyyy-MM-dd
            try {
                LocalDate.parse(s, DateTimeFormatter.ISO_LOCAL_DATE);
                return RepairOutcome.valid();
            } catch (DateTimeParseException ignored) {}
            // Try epoch millis as string
            try {
                long epoch = Long.parseLong(s);
                return RepairOutcome.repaired(epoch, "EPOCH_STRING_TO_LONG");
            } catch (NumberFormatException ignored) {}
            // Try common patterns: dd/MM/yyyy, MM-dd-yyyy, etc.
            String[] patterns = {"dd/MM/yyyy", "MM/dd/yyyy", "yyyy/MM/dd",
                                 "dd-MM-yyyy", "MM-dd-yyyy",
                                 "dd MMM yyyy", "MMM dd yyyy"};
            for (String pattern : patterns) {
                try {
                    LocalDate parsed = LocalDate.parse(s, DateTimeFormatter.ofPattern(pattern));
                    String iso = parsed.format(DateTimeFormatter.ISO_LOCAL_DATE);
                    return RepairOutcome.repaired(iso, "DATE_FORMAT_NORMALIZED");
                } catch (DateTimeParseException ignored) {}
            }
            return RepairOutcome.unconvertible("Cannot parse '" + s + "' as a date");
        }
        return RepairOutcome.unconvertible("Cannot convert " + value.getClass().getSimpleName() + " to date");
    }

    private RepairOutcome repairInteger(Object value, String esType) {
        if (value instanceof Integer || value instanceof Long) return RepairOutcome.valid();
        if (value instanceof Double || value instanceof Float) {
            return RepairOutcome.repaired(((Number) value).longValue(), "TRUNCATE_DECIMAL_TO_" + esType.toUpperCase());
        }
        if (value instanceof String) {
            String s = ((String) value).trim();
            try {
                long parsed = (long) Double.parseDouble(s);
                return RepairOutcome.repaired(parsed, "PARSE_STRING_TO_" + esType.toUpperCase());
            } catch (NumberFormatException e) {
                return RepairOutcome.unconvertible("Cannot parse '" + s + "' as " + esType);
            }
        }
        if (value instanceof Boolean) {
            return RepairOutcome.repaired(((Boolean) value) ? 1L : 0L, "BOOLEAN_TO_INTEGER");
        }
        return RepairOutcome.unconvertible("Cannot convert " + value.getClass().getSimpleName() + " to " + esType);
    }

    private RepairOutcome repairDecimal(Object value, String esType) {
        if (value instanceof Double || value instanceof Float) return RepairOutcome.valid();
        if (value instanceof Number) {
            return RepairOutcome.repaired(((Number) value).doubleValue(), "NUMERIC_TO_" + esType.toUpperCase());
        }
        if (value instanceof String) {
            String s = ((String) value).trim();
            try {
                double parsed = Double.parseDouble(s);
                return RepairOutcome.repaired(parsed, "PARSE_STRING_TO_" + esType.toUpperCase());
            } catch (NumberFormatException e) {
                return RepairOutcome.unconvertible("Cannot parse '" + s + "' as " + esType);
            }
        }
        return RepairOutcome.unconvertible("Cannot convert " + value.getClass().getSimpleName() + " to " + esType);
    }

    private RepairOutcome repairBoolean(Object value) {
        if (value instanceof Boolean) return RepairOutcome.valid();
        if (value instanceof String) {
            String s = ((String) value).trim().toLowerCase();
            if (s.equals("true")  || s.equals("1") || s.equals("yes"))
                return RepairOutcome.repaired(true,  "PARSE_STRING_TO_BOOLEAN");
            if (s.equals("false") || s.equals("0") || s.equals("no"))
                return RepairOutcome.repaired(false, "PARSE_STRING_TO_BOOLEAN");
            return RepairOutcome.unconvertible("Cannot parse '" + s + "' as boolean");
        }
        if (value instanceof Number) {
            return RepairOutcome.repaired(((Number) value).intValue() != 0, "NUMERIC_TO_BOOLEAN");
        }
        return RepairOutcome.unconvertible("Cannot convert " + value.getClass().getSimpleName() + " to boolean");
    }

    private RepairOutcome repairText(Object value, String esType) {
        if (value instanceof String) return RepairOutcome.valid();
        if (value instanceof Number || value instanceof Boolean) {
            return RepairOutcome.repaired(String.valueOf(value), "SCALAR_TO_" + esType.toUpperCase());
        }
        try {
            String json = mapper.writeValueAsString(value);
            return RepairOutcome.repaired(json, "OBJECT_SERIALIZED_TO_" + esType.toUpperCase());
        } catch (Exception e) {
            return RepairOutcome.unconvertible("Cannot serialize to " + esType + ": " + e.getMessage());
        }
    }
}
