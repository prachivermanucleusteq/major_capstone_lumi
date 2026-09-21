package com.lumi.ingestion.exception;

public class InputFileNotFoundException extends RuntimeException{
    public InputFileNotFoundException(String message){
        super(message);
    }
}
