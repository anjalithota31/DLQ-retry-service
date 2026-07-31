package com.tyss.dlq;

import java.util.List;
import java.util.Map;

/**
 * Result of repairing a document.
 * - document            : cleaned doc with repaired fields (unconvertible fields removed)
 * - repairedFields      : fields that were successfully type-converted
 * - removedFields       : fields dropped for non-type reasons (e.g. structural)
 * - unconvertibleFields : fields that could not be converted; stored with reason in the raw index
 */
public class RepairResult {

    private final Map<String, Object>           document;
    private final List<RepairAction>            repairedFields;
    private final List<RemovedField>            removedFields;
    /** fieldName → full unconvertible detail (original value + esType + reason) */
    private final Map<String, UnconvertibleField> unconvertibleFields;

    public RepairResult(Map<String, Object> document,
                        List<RepairAction> repairedFields,
                        List<RemovedField> removedFields,
                        Map<String, UnconvertibleField> unconvertibleFields) {
        this.document            = document;
        this.repairedFields      = repairedFields;
        this.removedFields       = removedFields;
        this.unconvertibleFields = unconvertibleFields;
    }

    public Map<String, Object>           getDocument()            { return document; }
    public List<RepairAction>            getRepairedFields()      { return repairedFields; }
    public List<RemovedField>            getRemovedFields()       { return removedFields; }
    public Map<String, UnconvertibleField> getUnconvertibleFields() { return unconvertibleFields; }

    /** Carries the original value, expected ES type, and failure reason for one unconvertible field. */
    public static class UnconvertibleField {
        private final String originalValue;
        private final String esType;
        private final String reason;

        public UnconvertibleField(String originalValue, String esType, String reason) {
            this.originalValue = originalValue;
            this.esType        = esType;
            this.reason        = reason;
        }

        public String getOriginalValue() { return originalValue; }
        public String getEsType()        { return esType; }
        public String getReason()        { return reason; }
    }
}
