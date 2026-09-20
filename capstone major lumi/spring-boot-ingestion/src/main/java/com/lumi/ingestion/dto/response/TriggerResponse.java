package com.lumi.ingestion.dto.response;

import java.util.List;

public class TriggerResponse {

    private String executionId;
    private List<String> dagRunIds;
    private List<String> inputFiles;
    private String status;

    public TriggerResponse(String executionId, List<String> dagRunIds, List<String> inputFiles, String status) {
        this.executionId = executionId;
        this.dagRunIds   = dagRunIds;
        this.inputFiles  = inputFiles;
        this.status      = status;
    }

    public String getExecutionId()      { return executionId; }
    public List<String> getDagRunIds()  { return dagRunIds; }
    public List<String> getInputFiles() { return inputFiles; }
    public String getStatus()           { return status; }
}
