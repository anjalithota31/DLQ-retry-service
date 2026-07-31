package com.tyss.dlq;

/** Audit record for a field that could not be repaired and was removed. */
public class RemovedField {

    private final String fieldName;
    private final String esType;
    private final String originalValue;
    private final String reason;

    public RemovedField(String fieldName, String esType, String originalValue, String reason) {
        this.fieldName     = fieldName;
        this.esType        = esType;
        this.originalValue = originalValue;
        this.reason        = reason;
    }

    public String getFieldName()     { return fieldName; }
    public String getEsType()        { return esType; }
    public String getOriginalValue() { return originalValue; }
    public String getReason()        { return reason; }
}
