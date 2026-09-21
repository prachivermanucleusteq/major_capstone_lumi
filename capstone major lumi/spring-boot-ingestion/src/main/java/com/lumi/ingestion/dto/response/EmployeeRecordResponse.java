package com.lumi.ingestion.dto.response;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public record EmployeeRecordResponse(
        UUID employeeId,
        String firstName,
        String lastName,
        String email,
        String phoneNumber,
        LocalDate hireDate,
        String department,
        String jobTitle,
        BigDecimal salary,
        String currency,
        String employmentStatus,
        String managerId,
        Boolean active,
        List<String> skills,
        Map<String, Object> address,
        EmergencyContact emergencyContact,
        Instant ingestionTimestamp,
        String executionId,
        Instant sourceCreationTime) {
    public record EmergencyContact(String name, String relationship, String phone, String email) {
    }
}
