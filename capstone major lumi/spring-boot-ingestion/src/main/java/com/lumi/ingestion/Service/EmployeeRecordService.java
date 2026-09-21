package com.lumi.ingestion.service;

import com.lumi.ingestion.dto.response.EmployeeRecordResponse;
import com.lumi.ingestion.repository.EmployeeRecordRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
@RequiredArgsConstructor
@Slf4j
public class EmployeeRecordService {

    private final EmployeeRecordRepository employeeRecordRepository;

    public List<EmployeeRecordResponse> findRecords() {
        log.info("Fetching employee records");
        List<EmployeeRecordResponse> records = employeeRecordRepository.findAll();
        log.info("Employee records fetched successfully: count={}", records.size());
        return records;
    }
}