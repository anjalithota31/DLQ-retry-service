package com.tyss.dlq;

import java.time.Instant;

/**
 * Success record stored in the failed-documents index for tracking.
 * Contains indexName and _id for successfully repaired and indexed documents.
 */
public class SuccessfulDocument {

    private final String indexName;
    private final String documentId;
    private final String status;
    private final String succeededAt;

    public SuccessfulDocument(String indexName, String documentId) {
        this.indexName  = indexName;
        this.documentId = documentId;
        this.status     = "SUCCESS";
        this.succeededAt = Instant.now().toString();
    }

    public String getIndexName()   { return indexName; }
    public String getDocumentId()  { return documentId; }
    public String getStatus()      { return status; }
    public String getSucceededAt() { return succeededAt; }
}
