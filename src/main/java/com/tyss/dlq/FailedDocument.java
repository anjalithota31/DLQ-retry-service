package com.tyss.dlq;

import java.time.Instant;
import java.util.Map;

/**
 * Failure record stored in the failed-documents index.
 * Contains indexName, _id, failure reason, problematic fields with reasons, ES error details,
 * and original document content for debugging purposes.
 */
public class FailedDocument {

    private final String indexName;
    private final String documentId;
    private final String status;
    private final String failureReason;
    private final Map<String, String> problematicFields;
    private final String esErrorDetails;
    private final String failedAt;
    private final Map<String, Object> originalDocument;

    public FailedDocument(String indexName, String documentId, String failureReason, Map<String, String> problematicFields) {
        this(indexName, documentId, failureReason, problematicFields, null, null);
    }

    public FailedDocument(String indexName, String documentId, String failureReason, String esErrorDetails) {
        this(indexName, documentId, failureReason, null, esErrorDetails, null);
    }

    public FailedDocument(String indexName, String documentId, String failureReason, Map<String, String> problematicFields,
                         String esErrorDetails, Map<String, Object> originalDocument) {
        this.indexName         = indexName;
        this.documentId        = documentId;
        this.status            = "FAILED";
        this.failureReason     = failureReason;
        this.problematicFields = problematicFields;
        this.esErrorDetails    = esErrorDetails;
        this.failedAt          = Instant.now().toString();
        this.originalDocument  = originalDocument;
    }

    public String getIndexName()         { return indexName; }
    public String getDocumentId()        { return documentId; }
    public String getStatus()            { return status; }
    public String getFailureReason()     { return failureReason; }
    public Map<String, String> getProblematicFields() { return problematicFields; }
    public String getEsErrorDetails()    { return esErrorDetails; }
    public String getFailedAt()          { return failedAt; }
    public Map<String, Object> getOriginalDocument() { return originalDocument; }
}
