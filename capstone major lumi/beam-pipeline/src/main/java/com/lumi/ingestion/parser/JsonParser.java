package com.lumi.ingestion.parser;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
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

public class JsonParser extends DoFn<String, EmployeeRecord> {

    private static final Logger LOG = LoggerFactory.getLogger(JsonParser.class);
    public static final TupleTag<EmployeeRecord> VALID_TAG = new TupleTag<>() {};
    public static final TupleTag<ErrorRecord> ERROR_TAG = new TupleTag<>() {};
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private final String executionId;
    private final String sourceFile;

    public JsonParser(String executionId, String sourceFile) {
        this.executionId = executionId;
        this.sourceFile = sourceFile;
    }

    @ProcessElement
    public void processElement(@Element String rawJson, MultiOutputReceiver out) {
        JsonNode root;
        try {
            root = MAPPER.readTree(rawJson);
        } catch (Exception e) {
            LOG.warn("Failed to parse JSON input: {}", e.getMessage());
            out.get(ERROR_TAG).output(new ErrorRecord(executionId, sourceFile, e.getMessage(), Instant.now()));
            return;
        }

        if (root.isArray()) {
            for (JsonNode element : root) {
                parseElement(element, out);
            }
        } else {
            parseElement(root, out);
        }
    }

    private void parseElement(JsonNode node, MultiOutputReceiver out) {
        try {
            EmployeeRecord record = new EmployeeRecord();
            record.setEmployeeId(parseUuid(node, "employee_id"));
            record.setFirstName(parseString(node, "first_name"));
            record.setLastName(parseString(node, "last_name"));
            record.setEmail(parseString(node, "email"));
            record.setPhoneNumber(parseString(node, "phone_number"));
            record.setHireDate(parseDate(node, "hire_date"));
            record.setDepartment(parseString(node, "department"));
            record.setJobTitle(parseString(node, "job_title"));
            record.setSalary(parseDecimal(node, "salary"));
            record.setCurrency(parseString(node, "currency"));
            record.setEmploymentStatus(parseString(node, "employment_status"));
            record.setManagerId(parseUuid(node, "manager_id"));
            record.setIsActive(parseBoolean(node, "is_active"));
            record.setSkills(parseSkills(node));
            record.setAddress(parseJsonb(node, "address"));
            record.setEmergencyContact(parseJsonb(node, "emergency_contact"));
            out.get(VALID_TAG).output(record);

        } catch (Exception e) {
            LOG.warn("Failed to parse JSON employee record: {}", e.getMessage());
            out.get(ERROR_TAG).output(new ErrorRecord(executionId, sourceFile, e.getMessage(), Instant.now()));
        }
    }

    private String parseString(JsonNode node, String field) {
        JsonNode value = node.get(field);
        if (value == null || value.isNull()) {
            return " ";
        }
        return value.asText(" ");
    }

    private UUID parseUuid(JsonNode node, String field) {
        JsonNode value = node.get(field);
        if (value == null || value.isNull()) {
            return null;
        }
        String text = value.asText();
        try {
            return UUID.fromString(text);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Invalid UUID for field '" + field + "': " + text);
        }
    }

    private LocalDate parseDate(JsonNode node, String field) {
        JsonNode value = node.get(field);
        if (value == null || value.isNull()) {
            return null;
        }
        try {
            return LocalDate.parse(value.asText());
        } catch (Exception e) {
            throw new IllegalArgumentException("Invalid date for field '" + field + "': " + value.asText()
            );
        }
    }

    private BigDecimal parseDecimal(JsonNode node, String field) {
        JsonNode value = node.get(field);
        if (value == null || value.isNull()) {
            return null;
        }
        try {
            return value.decimalValue();
        } catch (Exception e) {
            throw new IllegalArgumentException("Invalid decimal for field '" + field + "': " + value.asText());
        }
    }

    private Boolean parseBoolean(JsonNode node, String field) {
        JsonNode value = node.get(field);
        if (value == null || value.isNull()) {
            return null;
        }
        if (!value.isBoolean()) {
            throw new IllegalArgumentException("Invalid boolean for field '" + field + "': " + value.asText());
        }
        return value.booleanValue();
    }

    private List<String> parseSkills(JsonNode node) {
        JsonNode value = node.get("skills");
        if (value == null || value.isNull() || !value.isArray()) {
            return new ArrayList<>();
        }
        List<String> skills = new ArrayList<>();
        value.forEach(skill -> skills.add(skill.asText()));
        return skills;
    }

    private String parseJsonb(JsonNode node, String field) {
        JsonNode value = node.get(field);
        if (value == null || value.isNull()) {
            return null;
        }
        return value.toString();
    }
}
