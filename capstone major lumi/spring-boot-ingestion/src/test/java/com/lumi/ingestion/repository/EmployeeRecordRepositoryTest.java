package com.lumi.ingestion.repository;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.lumi.ingestion.dto.response.EmployeeRecordResponse;
import com.lumi.ingestion.exception.EmployeeRecordException;
import com.lumi.ingestion.util.SensitiveDataDecryptor;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentMatchers;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;

import java.math.BigDecimal;
import java.sql.Array;
import java.sql.ResultSet;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class EmployeeRecordRepositoryTest {

    @Mock
    private JdbcTemplate jdbcTemplate;

    @Mock
    private SensitiveDataDecryptor decryptor;

    private EmployeeRecordRepository repository;

    @BeforeEach
    void setUp() {
        repository = new EmployeeRecordRepository(jdbcTemplate, new ObjectMapper(), decryptor);
    }

    @Test
    void findAll_mapsEncryptedAndStructuredFields() throws Exception {
        ResultSet resultSet = mock(ResultSet.class);
        Array skills = mock(Array.class);
        UUID employeeId = UUID.randomUUID();
        Instant ingestionTime = Instant.parse("2026-01-01T10:00:00Z");
        Instant sourceTime = Instant.parse("2025-12-31T10:00:00Z");
        when(resultSet.getString("emergency_contact")).thenReturn(
                "{\"name\":\"Sam\",\"relationship\":\"Parent\",\"phone\":\"encrypted-emergency\",\"email\":\"sam@example.com\"}");
        when(resultSet.getArray("skills")).thenReturn(skills);
        when(skills.getArray()).thenReturn(new String[]{"Java", "SQL"});
        when(resultSet.getString("phone_number")).thenReturn("encrypted-phone");
        when(resultSet.getString("salary")).thenReturn("encrypted-salary");
        when(resultSet.getString("address")).thenReturn("{\"city\":\"Indore\"}");
        when(resultSet.getObject("employee_id", UUID.class)).thenReturn(employeeId);
        when(resultSet.getObject("hire_date", LocalDate.class)).thenReturn(LocalDate.of(2024, 1, 15));
        when(resultSet.getTimestamp("ingestion_timestamp")).thenReturn(Timestamp.from(ingestionTime));
        when(resultSet.getTimestamp("source_creation_time")).thenReturn(Timestamp.from(sourceTime));
        when(resultSet.getString("first_name")).thenReturn("Ada");
        when(resultSet.getString("last_name")).thenReturn("Lovelace");
        when(resultSet.getBoolean("is_active")).thenReturn(true);
        when(decryptor.decrypt("encrypted-phone")).thenReturn("555-0100");
        when(decryptor.decrypt("encrypted-salary")).thenReturn("125000.50");
        when(decryptor.decrypt("encrypted-emergency")).thenReturn("555-0199");
        stubQuery(resultSet);

        EmployeeRecordResponse record = repository.findAll().get(0);

        assertEquals(employeeId, record.employeeId());
        assertEquals("Ada", record.firstName());
        assertEquals("555-0100", record.phoneNumber());
        assertEquals(new BigDecimal("125000.50"), record.salary());
        assertEquals(List.of("Java", "SQL"), record.skills());
        assertEquals("Indore", record.address().get("city"));
        assertEquals("555-0199", record.emergencyContact().phone());
        assertEquals(ingestionTime, record.ingestionTimestamp());
        assertEquals(sourceTime, record.sourceCreationTime());
    }

    @Test
    void findAll_handlesNullAndBlankOptionalColumns() throws Exception {
        ResultSet resultSet = mock(ResultSet.class);
        when(resultSet.getString("emergency_contact")).thenReturn(null);
        when(resultSet.getArray("skills")).thenReturn(null);
        when(resultSet.getString("phone_number")).thenReturn(" ");
        when(resultSet.getString("salary")).thenReturn("");
        when(resultSet.getString("address")).thenReturn(" ");
        when(resultSet.getTimestamp("ingestion_timestamp")).thenReturn(null);
        when(resultSet.getTimestamp("source_creation_time")).thenReturn(null);
        stubQuery(resultSet);

        EmployeeRecordResponse record = repository.findAll().get(0);

        assertEquals(List.of(), record.skills());
        assertNull(record.phoneNumber());
        assertNull(record.salary());
        assertNull(record.address());
        assertNotNull(record.emergencyContact());
        assertNull(record.emergencyContact().phone());
        assertNull(record.ingestionTimestamp());
        assertNull(record.sourceCreationTime());
    }

    @Test
    void findAll_wrapsDatabaseFailure() {
        when(jdbcTemplate.query(anyString(),
                ArgumentMatchers.<RowMapper<EmployeeRecordResponse>>any()))
                .thenThrow(new DataAccessResourceFailureException("database unavailable"));

        EmployeeRecordException exception = assertThrows(EmployeeRecordException.class, repository::findAll);

        assertEquals("Unable to retrieve employee records", exception.getMessage());
    }

    private void stubQuery(ResultSet resultSet) throws Exception {
        when(jdbcTemplate.query(anyString(),
                ArgumentMatchers.<RowMapper<EmployeeRecordResponse>>any()))
                .thenAnswer(invocation -> {
                    RowMapper<EmployeeRecordResponse> mapper = invocation.getArgument(1);
                    return List.of(mapper.mapRow(resultSet, 1));
                });
    }
}