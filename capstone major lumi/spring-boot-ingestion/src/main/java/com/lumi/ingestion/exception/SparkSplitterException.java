package com.lumi.ingestion.exception;

public class SparkSplitterException extends RuntimeException {

    public SparkSplitterException(String message) {
        super(message);
    }

    public SparkSplitterException(String message, Throwable cause) {
        super(message, cause);
    }
}