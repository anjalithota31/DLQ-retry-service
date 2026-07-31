package com.tyss.dlq;

import java.time.Instant;
import java.util.Map;

/**
 * Failure record stored in the failed-documents index.
 * Contains indexName, _id, failure reason, problematic fields with reasons, and ES error details for tracking purposes.
 */
public class FailedDocument {

    private final String indexName;
    private final String documentId;
    private final String status;
    private final String failureReason;
    private final Map<String, String> problematicFields;
    private final String esErrorDetails;
    private final String failedAt;

    public FailedDocument(String indexName, String documentId, String failureReason, Map<String, String> problematicFields) {
        this.indexName         = indexName;
        this.documentId        = documentId;
        this.status            = "FAILED";
        this.failureReason     = failureReason;
        this.problematicFields = problematicFields;
        this.esErrorDetails    = null;
        this.failedAt          = Instant.now().toString();
    }

    public FailedDocument(String indexName, String documentId, String failureReason, String esErrorDetails) {
        this.indexName         = indexName;
        this.documentId        = documentId;
        this.status            = "FAILED";
        this.failureReason     = failureReason;
        this.problematicFields = null;
        this.esErrorDetails    = esErrorDetails;
        this.failedAt          = Instant.now().toString();
    }

    public String getIndexName()         { return indexName; }
    public String getDocumentId()        { return documentId; }
    public String getStatus()            { return status; }
    public String getFailureReason()     { return failureReason; }
    public Map<String, String> getProblematicFields() { return problematicFields; }
    public String getEsErrorDetails()    { return esErrorDetails; }
    public String getFailedAt()          { return failedAt; }
}
