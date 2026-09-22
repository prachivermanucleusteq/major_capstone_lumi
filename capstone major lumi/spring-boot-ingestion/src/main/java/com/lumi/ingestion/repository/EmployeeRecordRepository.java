package com.lumi.ingestion.repository;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.lumi.ingestion.dto.response.EmployeeRecordResponse;
import com.lumi.ingestion.exception.EmployeeRecordException;
import com.lumi.ingestion.util.SensitiveDataDecryptor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.sql.Array;
import java.util.UUID;

@Repository
@Slf4j
public class EmployeeRecordRepository {

    private static final String FETCH_SQL = """
            SELECT * 
            FROM employee
            ORDER BY ingestion_timestamp DESC
            """;

    private final JdbcTemplate jdbcTemplate;
    private final ObjectMapper objectMapper;
    private final SensitiveDataDecryptor sensitiveDataDecryptor;

    public EmployeeRecordRepository(JdbcTemplate jdbcTemplate, ObjectMapper objectMapper,
                                   SensitiveDataDecryptor sensitiveDataDecryptor) {
        this.jdbcTemplate = jdbcTemplate;
        this.objectMapper = objectMapper;
        this.sensitiveDataDecryptor = sensitiveDataDecryptor;
    }

    public List<EmployeeRecordResponse> findAll() {
        try {
            return jdbcTemplate.query(FETCH_SQL, this::mapRecord);
        } catch (RuntimeException exception) {
            log.error("Employee record fetch failed while reading employee table", exception);
            throw new EmployeeRecordException("Unable to retrieve employee records", exception);
        }
    }

    private EmployeeRecordResponse mapRecord(ResultSet resultSet, int rowNumber) throws SQLException {
        try {
            String encryptedEmergencyJson = resultSet.getString("emergency_contact");
            Map<String, Object> emergency = null;
            if (encryptedEmergencyJson != null && !encryptedEmergencyJson.isBlank()) {
                emergency = readObject(encryptedEmergencyJson);
            }
            if (emergency != null) {
                String encryptedEmergencyPhone = text(emergency, "phone");
                if (encryptedEmergencyPhone != null && !encryptedEmergencyPhone.isBlank()) {
                    emergency.put("phone", sensitiveDataDecryptor.decrypt(encryptedEmergencyPhone));
                }
            }

            Array skillsArray = resultSet.getArray("skills");
            List<String> skills = (skillsArray == null)
                    ? List.of()
                    : Arrays.asList((String[]) skillsArray.getArray());

            String phoneNumber = resultSet.getString("phone_number");
            String decryptedPhoneNumber = (phoneNumber == null || phoneNumber.isBlank())
                    ? null
                    : sensitiveDataDecryptor.decrypt(phoneNumber);

            String salaryValue = resultSet.getString("salary");
            BigDecimal decodedSalary = decryptSalary(salaryValue);

            String addressJson = resultSet.getString("address");
            Map<String, Object> address = (addressJson == null || addressJson.isBlank())
                    ? null
                    : readObject(addressJson);

            java.sql.Timestamp ingestionTimestamp = resultSet.getTimestamp("ingestion_timestamp");
            java.sql.Timestamp sourceCreationTimestamp = resultSet.getTimestamp("source_creation_time");

            return new EmployeeRecordResponse(
                    resultSet.getObject("employee_id", UUID.class),
                    resultSet.getString("first_name"),
                    resultSet.getString("last_name"),
                    resultSet.getString("email"),
                    decryptedPhoneNumber,
                    resultSet.getObject("hire_date", java.time.LocalDate.class),
                    resultSet.getString("department"),
                    resultSet.getString("job_title"),
                    decodedSalary,
                    resultSet.getString("currency"),
                    resultSet.getString("employment_status"),
                    resultSet.getString("manager_id"),
                    resultSet.getBoolean("is_active"),
                    skills,
                    address,
                    new EmployeeRecordResponse.EmergencyContact(
                            text(emergency, "name"),
                            text(emergency, "relationship"),
                            (emergency == null) ? null : text(emergency, "phone"),
                            text(emergency, "email")
                    ),
                    ingestionTimestamp == null ? null : ingestionTimestamp.toInstant(),
                    resultSet.getString("execution_id"),
                    sourceCreationTimestamp == null ? null : sourceCreationTimestamp.toInstant()
            );

        } catch (Exception exception) {
            throw new SQLException("Unable to map employee record at row " + rowNumber, exception);
        }
    }

    private BigDecimal decryptSalary(String encryptedSalary) {
        if (encryptedSalary == null || encryptedSalary.isBlank()) {
            return null;
        }
        String salary = sensitiveDataDecryptor.decrypt(encryptedSalary);
        return salary == null || salary.isBlank() ? null : new BigDecimal(salary);
    }

    private List<String> readList(String value) throws Exception {
        return value == null ? null : objectMapper.readValue(value, objectMapper.getTypeFactory().constructCollectionType(List.class, String.class));
    }

    private Map<String, Object> readObject(String value) throws Exception {
        return value == null ? null : objectMapper.readValue(value, objectMapper.getTypeFactory().constructMapType(Map.class, String.class, Object.class));
    }

    private String text(Map<String, Object> object, String field) {
        Object value = object == null ? null : object.get(field);
        return value == null ? null : String.valueOf(value);
    }
}