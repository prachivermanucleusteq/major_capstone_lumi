package com.lumi.ingestion.exception;

public class EmployeeRecordException extends RuntimeException {

    public EmployeeRecordException(String message, Throwable cause) {
        super(message, cause);
    }
}