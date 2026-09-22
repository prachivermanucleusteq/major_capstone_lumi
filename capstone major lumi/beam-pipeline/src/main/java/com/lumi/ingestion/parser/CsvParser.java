package com.lumi.ingestion.parser;

import com.lumi.ingestion.error.ErrorRecord;
import com.lumi.ingestion.model.EmployeeRecord;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.TupleTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class CsvParser extends DoFn<String, EmployeeRecord> {

    private static final Logger LOG = LoggerFactory.getLogger(CsvParser.class);
    public static final TupleTag<EmployeeRecord> VALID_TAG = JsonParser.VALID_TAG;
    public static final TupleTag<ErrorRecord>    ERROR_TAG = JsonParser.ERROR_TAG;
    private final String executionId;
    private final String sourceFile;

    public CsvParser(String executionId, String sourceFile) {
        this.executionId = executionId;
        this.sourceFile  = sourceFile;
    }

    @ProcessElement
    public void processElement(@Element String input, MultiOutputReceiver out) {
        int sep = input.indexOf('\t');
        if (sep < 0) {
            out.get(ERROR_TAG).output(new ErrorRecord(executionId, sourceFile, "CsvParser: missing header/row separator", Instant.now()));
            return;
        }
        String headerLine = input.substring(0, sep);
        String dataLine   = input.substring(sep + 1);
        String[] headers = parseCsvLine(headerLine);
        String[] values  = parseCsvLine(dataLine);

        try {
            EmployeeRecord record = new EmployeeRecord();
            record.setEmployeeId(      parseUuid(   col(headers, values, "employee_id")));
            record.setFirstName(       parseString( col(headers, values, "first_name")));
            record.setLastName(        parseString( col(headers, values, "last_name")));
            record.setEmail(           parseString( col(headers, values, "email")));
            record.setPhoneNumber(     parseString( col(headers, values, "phone_number")));
            record.setHireDate(        parseDate(   col(headers, values, "hire_date")));
            record.setDepartment(      parseString( col(headers, values, "department")));
            record.setJobTitle(        parseString( col(headers, values, "job_title")));
            record.setSalary(          parseDecimal(col(headers, values, "salary")));
            record.setCurrency(        parseString( col(headers, values, "currency")));
            record.setEmploymentStatus(parseString( col(headers, values, "employment_status")));
            record.setManagerId(       parseUuid(   col(headers, values, "manager_id")));
            record.setIsActive(        parseBoolean(col(headers, values, "is_active")));
            record.setSkills(          parseSkills( col(headers, values, "skills")));
            record.setAddress(         parseJsonb(  col(headers, values, "address")));
            record.setEmergencyContact(parseJsonb(  col(headers, values, "emergency_contact")));
            record.setSourceCreationTime(Instant.now());
            out.get(VALID_TAG).output(record);
        } catch (Exception e) {
            LOG.warn("Failed to parse CSV row: {}", e.getMessage());
            out.get(ERROR_TAG).output(new ErrorRecord(
                    executionId, sourceFile, e.getMessage(), Instant.now()));
        }
    }

    private String col(String[] headers, String[] values, String name) {
        for (int i = 0; i < headers.length; i++) {
            if (headers[i].trim().equalsIgnoreCase(name)) {
                return (i < values.length) ? values[i] : null;
            }
        }
        return null;
    }

    private String parseString(String raw) {
        if (raw == null || raw.isEmpty()) return " ";
        return raw;
    }

    private UUID parseUuid(String raw) {
        if (raw == null || raw.isEmpty()) return null;
        try {
            return UUID.fromString(raw);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Invalid UUID: " + raw);
        }
    }

    private LocalDate parseDate(String raw) {
        if (raw == null || raw.isEmpty()) return null;
        try {
            return LocalDate.parse(raw);
        } catch (Exception e) {
            throw new IllegalArgumentException("Invalid date: " + raw);
        }
    }

    private BigDecimal parseDecimal(String raw) {
        if (raw == null || raw.isEmpty()) return null;
        try {
            return new BigDecimal(raw);
        } catch (Exception e) {
            throw new IllegalArgumentException("Invalid decimal: " + raw);
        }
    }

    private Boolean parseBoolean(String raw) {
        if (raw == null || raw.isEmpty()) return null;
        if ("true".equalsIgnoreCase(raw))  return Boolean.TRUE;
        if ("false".equalsIgnoreCase(raw)) return Boolean.FALSE;
        throw new IllegalArgumentException("Invalid boolean: " + raw);
    }

    private List<String> parseSkills(String raw) {
        if (raw == null || raw.isEmpty()) return new ArrayList<>();
        // Strip outer brackets
        String trimmed = raw.trim();
        if (trimmed.startsWith("[")) trimmed = trimmed.substring(1);
        if (trimmed.endsWith("]"))   trimmed = trimmed.substring(0, trimmed.length() - 1);
        if (trimmed.isEmpty()) return new ArrayList<>();
        List<String> skills = new ArrayList<>();
        for (String part : trimmed.split(",")) {
            String s = part.trim();
            if (s.startsWith("\"")) s = s.substring(1);
            if (s.endsWith("\""))   s = s.substring(0, s.length() - 1);
            if (!s.isEmpty()) skills.add(s);
        }
        return skills;
    }

    private String parseJsonb(String raw) {
        if (raw == null || raw.isEmpty()) return null;
        return raw;
    }

    static String[] parseCsvLine(String line) {
        List<String> fields = new ArrayList<>();
        StringBuilder sb = new StringBuilder();
        boolean inQuotes = false;
        for (int i = 0; i < line.length(); i++) {
            char c = line.charAt(i);
            if (inQuotes) {
                if (c == '"') {
                    if (i + 1 < line.length() && line.charAt(i + 1) == '"') {
                        sb.append('"');
                        i++;
                    } else {
                        inQuotes = false;
                    }
                } else {
                    sb.append(c);
                }
            } else {
                if (c == '"') {
                    inQuotes = true;
                } else if (c == ',') {
                    fields.add(sb.toString());
                    sb.setLength(0);
                } else {
                    sb.append(c);
                }
            }
        }
        fields.add(sb.toString());
        return fields.toArray(new String[0]);
    }
}