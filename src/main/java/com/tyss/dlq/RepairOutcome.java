package com.tyss.dlq;

/**
 * Outcome of attempting to repair a single field.
 */
public class RepairOutcome {

    public enum Status { VALID, REPAIRED, REMOVED, UNCONVERTIBLE }

    private final Status  status;
    private final Object  repairedValue;
    private final String  strategy;
    private final String  reason;

    private RepairOutcome(Status status, Object repairedValue, String strategy, String reason) {
        this.status        = status;
        this.repairedValue = repairedValue;
        this.strategy      = strategy;
        this.reason        = reason;
    }

    public static RepairOutcome valid() {
        return new RepairOutcome(Status.VALID, null, null, null);
    }

    public static RepairOutcome repaired(Object value, String strategy) {
        return new RepairOutcome(Status.REPAIRED, value, strategy, null);
    }

    public static RepairOutcome removed(String reason) {
        return new RepairOutcome(Status.REMOVED, null, null, reason);
    }

    public static RepairOutcome unconvertible(String reason) {
        return new RepairOutcome(Status.UNCONVERTIBLE, null, null, reason);
    }

    public Status getStatus()        { return status; }
    public Object getRepairedValue() { return repairedValue; }
    public String getStrategy()      { return strategy; }
    public String getReason()        { return reason; }
}
