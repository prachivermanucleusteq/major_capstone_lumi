package com.lumi.ingestion.controller;

import com.lumi.ingestion.dto.response.EmployeeRecordResponse;
import com.lumi.ingestion.exception.EmployeeRecordException;
import com.lumi.ingestion.service.EmployeeRecordService;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.web.servlet.MockMvc;

import java.util.List;
import java.util.UUID;

import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@WebMvcTest(EmployeeRecordController.class)
class EmployeeRecordControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @MockBean
    private EmployeeRecordService employeeRecordService;

    @Test
    void getRecords_returnsEmployeeRecords() throws Exception {
        EmployeeRecordResponse record = new EmployeeRecordResponse(
                UUID.fromString("a33090ea-770c-4c3d-ac98-76694aa1c3fb"),
                "Ada", "Lovelace", "ada@example.com", "555-0100", null,
                "Engineering", "Analyst", null, "USD", "ACTIVE", null,
                true, List.of("Java"), null, null, null, "run-1", null);
        when(employeeRecordService.findRecords()).thenReturn(List.of(record));

        mockMvc.perform(get("/api/employee-records"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$[0].employeeId").value(record.employeeId().toString()))
                .andExpect(jsonPath("$[0].firstName").value("Ada"))
                .andExpect(jsonPath("$[0].skills[0]").value("Java"));
    }

    @Test
    void getRecords_returnsServerErrorWhenServiceFails() throws Exception {
        when(employeeRecordService.findRecords()).thenThrow(
                new EmployeeRecordException("database unavailable", new RuntimeException()));

        mockMvc.perform(get("/api/employee-records"))
                .andExpect(status().isInternalServerError())
                .andExpect(jsonPath("$.status").value(500))
                .andExpect(jsonPath("$.message").value("Unable to fetch employee records"))
                .andExpect(jsonPath("$.path").value("/api/employee-records"));
    }
}