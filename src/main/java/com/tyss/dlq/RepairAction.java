package com.tyss.dlq;

/** Audit record for a field that was successfully repaired. */
public class RepairAction {

    private final String fieldName;
    private final String esType;
    private final String originalValue;
    private final String repairedValue;
    private final String strategy;

    public RepairAction(String fieldName, String esType,
                        String originalValue, String repairedValue, String strategy) {
        this.fieldName     = fieldName;
        this.esType        = esType;
        this.originalValue = originalValue;
        this.repairedValue = repairedValue;
        this.strategy      = strategy;
    }

    public String getFieldName()     { return fieldName; }
    public String getEsType()        { return esType; }
    public String getOriginalValue() { return originalValue; }
    public String getRepairedValue() { return repairedValue; }
    public String getStrategy()      { return strategy; }
}
