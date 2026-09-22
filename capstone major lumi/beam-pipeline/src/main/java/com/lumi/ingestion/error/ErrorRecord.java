package com.lumi.ingestion.error;

import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.coders.SerializableCoder;

import java.io.Serializable;
import java.time.Instant;

@DefaultCoder(SerializableCoder.class)
public class ErrorRecord implements Serializable {

    private String executionId;
    private String sourceFile;
    private String errorReason;
    private Instant timestamp;

    public ErrorRecord(String executionId, String sourceFile, String errorReason, Instant timestamp) {
        this.executionId = executionId;
        this.sourceFile = sourceFile;
        this.errorReason = errorReason;
        this.timestamp = timestamp;
    }

    public String getExecutionId() { return executionId; }
    public String getSourceFile() { return sourceFile; }
    public String getErrorReason() { return errorReason; }
    public Instant getTimestamp() { return timestamp; }

    @Override
    public String toString() {
        return "{\"executionId\":\"" + executionId + "\""
             + ",\"sourceFile\":\"" + sourceFile + "\""
             + ",\"errorReason\":\"" + errorReason.replace("\"", "'") + "\""
             + ",\"timestamp\":\"" + timestamp + "\""
             + "}";
    }
}