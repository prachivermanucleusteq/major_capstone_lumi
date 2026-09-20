package com.lumi.ingestion.dto.request;

import jakarta.validation.constraints.NotBlank;

public class TriggerRequest {

    @NotBlank(message = "controlFile is required")
    private String controlFile;

    public String getControlFile() { return controlFile; }
    public void setControlFile(String controlFile) { this.controlFile = controlFile; }
}
