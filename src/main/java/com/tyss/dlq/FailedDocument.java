package com.tyss.dlq;

import java.time.Instant;
import java.util.Map;

/**
 * Document status record stored in the dlq-documents-status index.
 * Tracks all documents (SUCCESS/FAILED) with repair details, problematic fields,
 * repaired fields, removed fields, and original document content.
 */
public class FailedDocument {

    private final String indexName;
    private final String documentId;
    private final String status;
    private final String failureReason;
    private final Map<String, String> problematicFields;
    private final Map<String, String> repairedFields;
    private final Map<String, String> removedFields;
    private final String esErrorDetails;
    private final String processedAt;
    private final String originalDocument;

    public FailedDocument(String indexName, String documentId, String failureReason, Map<String, String> problematicFields) {
        this(indexName, documentId, failureReason, problematicFields, null, null, null, null, "FAILED");
    }

    public FailedDocument(String indexName, String documentId, String failureReason, String esErrorDetails) {
        this(indexName, documentId, failureReason, null, null, null, esErrorDetails, null, "FAILED");
    }

    public FailedDocument(String indexName, String documentId, String failureReason, Map<String, String> problematicFields,
                         String esErrorDetails, String originalDocument) {
        this(indexName, documentId, failureReason, problematicFields, null, null, esErrorDetails, originalDocument, "FAILED");
    }

    public FailedDocument(String indexName, String documentId, String failureReason, Map<String, String> problematicFields,
                         String esErrorDetails, String originalDocument, String status) {
        this(indexName, documentId, failureReason, problematicFields, null, null, esErrorDetails, originalDocument, status);
    }

    public FailedDocument(String indexName, String documentId, String failureReason, Map<String, String> problematicFields,
                         Map<String, String> repairedFields, Map<String, String> removedFields,
                         String esErrorDetails, String originalDocument, String status) {
        this.indexName         = indexName;
        this.documentId        = documentId;
        this.status            = status;
        this.failureReason     = failureReason;
        this.problematicFields = problematicFields;
        this.repairedFields    = repairedFields;
        this.removedFields     = removedFields;
        this.esErrorDetails    = esErrorDetails;
        this.processedAt       = Instant.now().toString();
        this.originalDocument  = originalDocument;
    }

    public String getIndexName()         { return indexName; }
    public String getDocumentId()        { return documentId; }
    public String getStatus()            { return status; }
    public String getFailureReason()     { return failureReason; }
    public Map<String, String> getProblematicFields() { return problematicFields; }
    public Map<String, String> getRepairedFields()    { return repairedFields; }
    public Map<String, String> getRemovedFields()     { return removedFields; }
    public String getEsErrorDetails()    { return esErrorDetails; }
    public String getProcessedAt()       { return processedAt; }
    public String getOriginalDocument()  { return originalDocument; }
}
