package com.lumi.ingestion.controller;

import com.lumi.ingestion.dto.request.TriggerRequest;
import com.lumi.ingestion.dto.response.TriggerResponse;
import com.lumi.ingestion.service.IngestionService;
import jakarta.validation.Valid;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/ingestion")
public class IngestionController {

    private final IngestionService ingestionService;
    public IngestionController(IngestionService ingestionService) {
        this.ingestionService = ingestionService;
    }

    @PostMapping("/trigger")
    public ResponseEntity<TriggerResponse> trigger(@Valid @RequestBody TriggerRequest request) {
        TriggerResponse response = ingestionService.trigger(request);
        return ResponseEntity.accepted().body(response);
    }
}