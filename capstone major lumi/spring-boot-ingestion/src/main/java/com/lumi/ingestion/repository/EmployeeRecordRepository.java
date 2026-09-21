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

    public EmployeeRecordRepository(JdbcTemplate jdbcTemplate, ObjectMapper objectMapper) {
        this.jdbcTemplate = jdbcTemplate;
        this.objectMapper = objectMapper;
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
            Map<String, Object> emergency = readObject(resultSet.getString("emergency_contact"));
            Array skillsArray = resultSet.getArray("skills");
            List<String> skills = skillsArray == null ? List.of() : Arrays.asList((String[]) skillsArray.getArray());
            return new EmployeeRecordResponse(
                    resultSet.getObject("employee_id", UUID.class),
                    resultSet.getString("first_name"),
                    resultSet.getString("last_name"),
                    resultSet.getString("email"),
                    SensitiveDataDecryptor.decrypt(resultSet.getString("phone_number")),
                    resultSet.getObject("hire_date", java.time.LocalDate.class),
                    resultSet.getString("department"),
                    resultSet.getString("job_title"),
                    decryptSalary(resultSet.getString("salary")),
                    resultSet.getString("currency"),
                    resultSet.getString("employment_status"),
                    resultSet.getString("manager_id"),
                    resultSet.getBoolean("is_active"),
                    skills,
                    readObject(resultSet.getString("address")),
                    new EmployeeRecordResponse.EmergencyContact(text(emergency, "name"), text(emergency, "relationship"),
                            SensitiveDataDecryptor.decrypt(text(emergency, "phone")),
                            text(emergency, "email")),
                    resultSet.getTimestamp("ingestion_timestamp").toInstant(),
                    resultSet.getString("execution_id"),
                    resultSet.getTimestamp("source_creation_time").toInstant()
            );

        } catch (Exception exception) {
            throw new SQLException("Unable to map employee record at row " + rowNumber, exception);
        }
    }

    private BigDecimal decryptSalary(String encryptedSalary) {
        String salary = SensitiveDataDecryptor.decrypt(encryptedSalary);
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