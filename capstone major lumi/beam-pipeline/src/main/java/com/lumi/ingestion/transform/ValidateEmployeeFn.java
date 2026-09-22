package com.lumi.ingestion.transform;

import com.lumi.ingestion.error.ErrorRecord;
import com.lumi.ingestion.model.EmployeeRecord;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.TupleTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

/* Validates an EmployeeRecord against the PostgreSQL schema constraints.*/
public class ValidateEmployeeFn extends DoFn<EmployeeRecord, EmployeeRecord> {

    private static final Logger LOG = LoggerFactory.getLogger(ValidateEmployeeFn.class);
    public static final TupleTag<EmployeeRecord> VALID_TAG = new TupleTag<>() {};
    public static final TupleTag<ErrorRecord> ERROR_TAG = new TupleTag<>() {};
    private final String executionId;
    private final String sourceFile;

    public ValidateEmployeeFn(String executionId, String sourceFile) {
        this.executionId = executionId;
        this.sourceFile = sourceFile;
    }

    @ProcessElement
    public void processElement(@Element EmployeeRecord record, MultiOutputReceiver out) {
        List<String> violations = new ArrayList<>();

        // employee_id is the primary key — must be present
        if (record.getEmployeeId() == null) {
            violations.add("employee_id is null (required primary key)");
        }

        // first_name: CHECK (char_length BETWEEN 3 AND 15)
        validateStringLength(record.getFirstName(), "first_name", 3, 15, violations);

        // last_name: CHECK (char_length BETWEEN 0 AND 15)
        validateStringLength(record.getLastName(), "last_name", 0, 15, violations);

        // email: CHECK (char_length BETWEEN 13 AND 30) + structural check
        validateStringLength(record.getEmail(), "email", 13, 30, violations);
        if (record.getEmail() != null && !record.getEmail().isBlank()) {
            String email = record.getEmail().trim();
            if (!email.contains("@") || !email.contains(".")) {
                violations.add("email does not contain '@' and '.'");
            }
        }

        // phone_number: CHECK (char_length BETWEEN 10 AND 15)
        validateStringLength(record.getPhoneNumber(), "phone_number", 10, 15, violations);

        // department: VARCHAR(20)
        validateStringLength(record.getDepartment(), "department", 0, 20, violations);

        // job_title: VARCHAR(30)
        validateStringLength(record.getJobTitle(), "job_title", 0, 30, violations);

        // currency: CHECK (char_length = 3)
        if (record.getCurrency() != null) {
            int len = record.getCurrency().trim().length();
            if (len != 3) {
                violations.add("currency must be exactly 3 characters, got: " + len);
            }
        }

        // employment_status: CHECK (char_length BETWEEN 3 AND 13)
        validateStringLength(record.getEmploymentStatus(), "employment_status", 3, 13, violations);

        // salary must be non-negative if present
        if (record.getSalary() != null && record.getSalary().signum() < 0) {
            violations.add("salary must be non-negative");
        }

        if (violations.isEmpty()) {
            out.get(VALID_TAG).output(record);
        } else {
            String reason = String.join("; ", violations);
            LOG.warn("Validation failed for record {}: {}", record.getEmployeeId(), reason);
            out.get(ERROR_TAG).output(new ErrorRecord(executionId, sourceFile, reason, Instant.now()));
        }
    }

    private void validateStringLength(String value, String field, int min, int max, List<String> violations) {
        if (value == null) return; // null is allowed at this layer; DB will enforce NOT NULL if needed
        int len = value.length();
        if (len < min || len > max) {
            violations.add(field + " length " + len + " not in [" + min + ", " + max + "]");
        }
    }
}