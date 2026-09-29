package com.lumi.ingestion;

import com.lumi.ingestion.error.ErrorRecord;
import com.lumi.ingestion.model.EmployeeRecord;
import com.lumi.ingestion.parser.CsvParser;
import com.lumi.ingestion.parser.JsonParser;
import com.lumi.ingestion.transform.EnrichEmployeeFn;
import com.lumi.ingestion.transform.ValidateEmployeeFn;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.TupleTag;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

class IngestionPipelineTest {

    private static final String EXECUTION_ID = "550e8400-e29b-41d4-a716-446655440000";
    private static final Instant SOURCE_CREATION_TIME = Instant.parse("2020-01-02T03:04:05Z");

    @Test
    void csvParser_shouldParseValidRow() {
        String header = "employee_id,first_name,last_name,email,phone_number,hire_date,department,job_title,salary,currency,employment_status,manager_id,is_active,skills,address,emergency_contact";
        String row = "550e8400-e29b-41d4-a716-446655440000,Alice,Johnson,alice.johnson@example.com,9876543210,2024-02-15,Engineering,Senior Engineer,120000.50,USD,ACTIVE,660e8400-e29b-41d4-a716-446655440001,true,\"[\"\"Java\"\",\"\"Beam\"\"]\",{},{}";
        List<EmployeeRecord> records = new ArrayList<>();
        List<ErrorRecord> errors = new ArrayList<>();
        new CsvParser(EXECUTION_ID, "employees.csv", SOURCE_CREATION_TIME)
                .processElement(header + "\t" + row,
                        collectingReceiver(CsvParser.VALID_TAG, CsvParser.ERROR_TAG, records, errors));

        assertEquals(1, records.size());
        EmployeeRecord employee = records.get(0);
        assertEquals(UUID.fromString(EXECUTION_ID), employee.getEmployeeId());
        assertEquals("Alice", employee.getFirstName());
        assertEquals("Johnson", employee.getLastName());
        assertEquals("alice.johnson@example.com", employee.getEmail());
        assertEquals("9876543210", employee.getPhoneNumber());
        assertEquals("Engineering", employee.getDepartment());
        assertEquals(0, new BigDecimal("120000.50").compareTo(employee.getSalary()));
        assertEquals(List.of("Java", "Beam"), employee.getSkills());
        assertEquals("{}", employee.getAddress());
        assertEquals("{}", employee.getEmergencyContact());
        assertEquals(SOURCE_CREATION_TIME, employee.getSourceCreationTime());
        assertTrue(errors.isEmpty());
    }

    @Test
    void jsonParser_shouldParseEmployeeArray() {
        String json = "[{\"employee_id\":\"550e8400-e29b-41d4-a716-446655440000\",\"first_name\":\"Alice\",\"last_name\":\"Johnson\",\"email\":\"alice.johnson@example.com\",\"phone_number\":\"9876543210\",\"hire_date\":\"2024-02-15\",\"department\":\"Engineering\",\"job_title\":\"Senior Engineer\",\"salary\":120000.50,\"currency\":\"USD\",\"employment_status\":\"ACTIVE\",\"manager_id\":\"660e8400-e29b-41d4-a716-446655440001\",\"is_active\":true,\"skills\":[\"Java\",\"Beam\"],\"address\":{\"city\":\"Indore\"},\"emergency_contact\":{\"phone\":\"9898989898\"}}]";
        List<EmployeeRecord> records = new ArrayList<>();
        List<ErrorRecord> errors = new ArrayList<>();
        new JsonParser(EXECUTION_ID, "employees.json", SOURCE_CREATION_TIME)
                .processElement(json,
                        collectingReceiver(JsonParser.VALID_TAG, JsonParser.ERROR_TAG, records, errors));

        assertEquals(1, records.size());
        EmployeeRecord employee = records.get(0);
        assertEquals(UUID.fromString(EXECUTION_ID), employee.getEmployeeId());
        assertEquals("Alice", employee.getFirstName());
        assertEquals("ACTIVE", employee.getEmploymentStatus());
        assertEquals(0, new BigDecimal("120000.50").compareTo(employee.getSalary()));
        assertEquals(List.of("Java", "Beam"), employee.getSkills());
        assertEquals("{\"city\":\"Indore\"}", employee.getAddress());
        assertEquals(SOURCE_CREATION_TIME, employee.getSourceCreationTime());
        assertTrue(errors.isEmpty());
    }

    @Test
    void validateEmployeeFn_shouldRejectInvalidEmail() {
        EmployeeRecord record = buildValidRecord();
        record.setEmail("bad-email");

        List<EmployeeRecord> validRecords = new ArrayList<>();
        List<ErrorRecord> errors = new ArrayList<>();

        ValidateEmployeeFn validator = new ValidateEmployeeFn(EXECUTION_ID, "employees.csv");
        validator.processElement(record, new DoFn.MultiOutputReceiver() {
            @Override
            public <T> DoFn.OutputReceiver<T> get(TupleTag<T> tag) {
                return new DoFn.OutputReceiver<>() {
                    @Override
                    public void output(T value) {
                        if (tag.equals(ValidateEmployeeFn.VALID_TAG)) {
                            validRecords.add((EmployeeRecord) value);
                        } else {
                            errors.add((ErrorRecord) value);
                        }
                    }

                    @Override
                    public void outputWithTimestamp(T value, org.joda.time.Instant timestamp) {
                        output(value);
                    }
                };
            }

            @Override
            public <T> DoFn.OutputReceiver<org.apache.beam.sdk.values.Row> getRowReceiver(TupleTag<T> tag) {
                throw new UnsupportedOperationException("Row output is not used by this test");
            }
        });

        assertTrue(validRecords.isEmpty());
        assertEquals(1, errors.size());
        assertTrue(errors.get(0).getErrorReason().contains("email"));
    }

    @Test
    void enrichEmployeeFn_shouldEncryptSensitiveFields() {
        EmployeeRecord record = buildValidRecord();
        record.setSalary(new BigDecimal("50000.00"));
        record.setPhoneNumber("9876543210");
        record.setEmergencyContact("{\"phone\":\"1234567890\"}");
        record.setSourceCreationTime(SOURCE_CREATION_TIME);

        List<EmployeeRecord> output = new ArrayList<>();
        List<ErrorRecord> errors = new ArrayList<>();
        EnrichEmployeeFn enrich = new EnrichEmployeeFn(
                EXECUTION_ID, "employees.csv", "12345678901234567890123456789012");
        enrich.processElement(record, new DoFn.MultiOutputReceiver() {
            @Override
            public <T> DoFn.OutputReceiver<T> get(TupleTag<T> tag) {
                return new DoFn.OutputReceiver<>() {
                    @Override
                    public void output(T value) {
                        if (tag.equals(EnrichEmployeeFn.VALID_TAG)) {
                            output.add((EmployeeRecord) value);
                        } else {
                            errors.add((ErrorRecord) value);
                        }
                    }

                    @Override
                    public void outputWithTimestamp(T value, org.joda.time.Instant timestamp) {
                        output(value);
                    }
                };
            }

            @Override
            public <T> DoFn.OutputReceiver<org.apache.beam.sdk.values.Row> getRowReceiver(TupleTag<T> tag) {
                throw new UnsupportedOperationException("Row output is not used by this test");
            }
        });

        assertEquals(1, output.size());
        assertTrue(errors.isEmpty());
        EmployeeRecord enriched = output.get(0);
        assertNotNull(enriched.getSalaryEncrypted());
        assertNotNull(enriched.getPhoneNumberEncrypted());
        assertNotNull(enriched.getEmergencyContactEncrypted());
        assertNotEquals(record.getPhoneNumber(), enriched.getPhoneNumberEncrypted());
        assertNotEquals(record.getSalary().toPlainString(), enriched.getSalaryEncrypted());
        assertTrue(enriched.getEmergencyContactEncrypted().contains("phone"));
        assertEquals(UUID.fromString(EXECUTION_ID), enriched.getExecutionId());
        assertNotNull(enriched.getIngestionTimestamp());
        assertEquals(SOURCE_CREATION_TIME, enriched.getSourceCreationTime());
        assertNotEquals(enriched.getIngestionTimestamp(), enriched.getSourceCreationTime());
    }

    @Test
    void enrichEmployeeFn_shouldRouteEncryptionFailureToErrorTag() {
        List<ErrorRecord> errors = new ArrayList<>();
        EnrichEmployeeFn enrich = new EnrichEmployeeFn(EXECUTION_ID, "employees.csv", "invalid-key");
        enrich.processElement(buildValidRecord(), new DoFn.MultiOutputReceiver() {
            @Override
            public <T> DoFn.OutputReceiver<T> get(TupleTag<T> tag) {
                return new DoFn.OutputReceiver<>() {
                    @Override
                    public void output(T value) {
                        errors.add((ErrorRecord) value);
                    }

                    @Override
                    public void outputWithTimestamp(T value, org.joda.time.Instant timestamp) {
                        output(value);
                    }
                };
            }

            @Override
            public <T> DoFn.OutputReceiver<org.apache.beam.sdk.values.Row> getRowReceiver(TupleTag<T> tag) {
                throw new UnsupportedOperationException("Row output is not used by this test");
            }
        });

        assertEquals(1, errors.size());
        assertEquals(EXECUTION_ID, errors.get(0).getExecutionId());
        assertEquals("employees.csv", errors.get(0).getSourceFile());
        assertTrue(errors.get(0).getErrorReason().contains("Enrichment failed"));
    }

    private EmployeeRecord buildValidRecord() {
        EmployeeRecord record = new EmployeeRecord();
        record.setEmployeeId(UUID.fromString(EXECUTION_ID));
        record.setFirstName("Alice");
        record.setLastName("Johnson");
        record.setEmail("alice.johnson@example.com");
        record.setPhoneNumber("9876543210");
        record.setHireDate(java.time.LocalDate.of(2024, 2, 15));
        record.setDepartment("Engineering");
        record.setJobTitle("Senior Engineer");
        record.setSalary(new BigDecimal("120000.50"));
        record.setCurrency("USD");
        record.setEmploymentStatus("ACTIVE");
        record.setManagerId(UUID.fromString("660e8400-e29b-41d4-a716-446655440001"));
        record.setIsActive(true);
        record.setSkills(List.of("Java", "Beam"));
        record.setAddress("{}")
        ;        record.setEmergencyContact("{}")
        ;        return record;
    }

    private DoFn.MultiOutputReceiver collectingReceiver(
            TupleTag<EmployeeRecord> validTag,
            TupleTag<ErrorRecord> errorTag,
            List<EmployeeRecord> validRecords,
            List<ErrorRecord> errors) {
        return new DoFn.MultiOutputReceiver() {
            @Override
            public <T> DoFn.OutputReceiver<T> get(TupleTag<T> tag) {
                return new DoFn.OutputReceiver<>() {
                    @Override
                    public void output(T value) {
                        if (tag.equals(validTag)) {
                            validRecords.add((EmployeeRecord) value);
                        } else if (tag.equals(errorTag)) {
                            errors.add((ErrorRecord) value);
                        } else {
                            throw new IllegalArgumentException("Unexpected output tag: " + tag);
                        }
                    }

                    @Override
                    public void outputWithTimestamp(T value, org.joda.time.Instant timestamp) {
                        output(value);
                    }
                };
            }

            @Override
            public <T> DoFn.OutputReceiver<org.apache.beam.sdk.values.Row> getRowReceiver(TupleTag<T> tag) {
                throw new UnsupportedOperationException("Row output is not used by these tests");
            }
        };
    }
}