package com.lumi.ingestion.controller;

import com.lumi.ingestion.dto.response.EmployeeRecordResponse;
import com.lumi.ingestion.service.EmployeeRecordService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequestMapping("/api/employee-records")
@RequiredArgsConstructor
public class EmployeeRecordController {

    private final EmployeeRecordService employeeRecordService;

    @GetMapping
    public ResponseEntity<List<EmployeeRecordResponse>> getRecords() {
        return ResponseEntity.ok(employeeRecordService.findRecords());
    }
}