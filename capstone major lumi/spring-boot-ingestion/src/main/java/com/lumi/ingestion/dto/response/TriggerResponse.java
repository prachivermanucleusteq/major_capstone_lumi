package com.lumi.ingestion.dto.response;

import java.util.List;

public class TriggerResponse {

    private String executionId;
    private String dagRunIds;
    private int inputFileCount;
    private boolean split;
    private String status;

    public TriggerResponse(String executionId, String dagRunIds, int inputFileCount, boolean split, String status) {
        this.executionId = executionId;
        this.dagRunIds   = dagRunIds;
        this.inputFileCount = inputFileCount;
        this.split = split;
        this.status      = status;
    }

    public String getExecutionId()      { return executionId; }
    public String getDagRunIds()  { return dagRunIds; }
    public int getInputFileCount(){return inputFileCount;}
    public boolean isSplit(){return split;}
    public String getStatus()           { return status; }
}
