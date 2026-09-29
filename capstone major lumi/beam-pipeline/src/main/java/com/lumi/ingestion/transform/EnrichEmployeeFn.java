package com.lumi.ingestion.transform;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.lumi.ingestion.model.EmployeeRecord;
import com.lumi.ingestion.error.ErrorRecord;
import com.lumi.ingestion.util.EncryptionUtil;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.TupleTag;

import java.time.Instant;
import java.util.UUID;

/* Adds pipeline-level metadata to each valid EmployeeRecord: */
public class EnrichEmployeeFn extends DoFn<EmployeeRecord, EmployeeRecord> {

    public static final TupleTag<EmployeeRecord> VALID_TAG = new TupleTag<>() {};
    public static final TupleTag<ErrorRecord> ERROR_TAG = new TupleTag<>() {};
    private final String executionId;
    private final String sourceFile;
    private final String encryptionKey;

    public EnrichEmployeeFn(String executionId, String sourceFile, String encryptionKey) {
        this.executionId   = executionId;
        this.sourceFile = sourceFile;
        this.encryptionKey = encryptionKey;
    }

    @ProcessElement
    public void processElement(@Element EmployeeRecord record, MultiOutputReceiver out) {
        try {
            EmployeeRecord enriched = new EmployeeRecord(record, Instant.now(), UUID.fromString(executionId));
            EncryptionUtil enc = new EncryptionUtil(encryptionKey);
            if (record.getSalary() != null) {
                enriched.setSalaryEncrypted(enc.encrypt(record.getSalary().toPlainString()));
            }
            if (record.getPhoneNumber() != null) {
                enriched.setPhoneNumberEncrypted(enc.encrypt(record.getPhoneNumber()));
            }
            if (record.getEmergencyContact() != null) {
                enriched.setEmergencyContactEncrypted(encryptEmergencyContactPhone(record.getEmergencyContact(), enc));
            }
            out.get(VALID_TAG).output(enriched);
        } catch (Exception e) {
            String employeeId = record.getEmployeeId() == null ? "unknown" : record.getEmployeeId().toString();
            out.get(ERROR_TAG).output(new ErrorRecord(
                    executionId, sourceFile,
                    "Enrichment failed for employee " + employeeId + ": " + e.getMessage(),
                    Instant.now()));
        }
    }

    private String encryptEmergencyContactPhone(String emergencyContactJson, EncryptionUtil enc) {
        try {
            ObjectMapper mapper = new ObjectMapper();
            JsonNode root = mapper.readTree(emergencyContactJson);
            JsonNode phoneNode = root.get("phone");
            if (phoneNode != null && !phoneNode.isNull()) {
                ((ObjectNode) root).put("phone", enc.encrypt(phoneNode.asText()));
            }
            return mapper.writeValueAsString(root);
        } catch (Exception e) {
            throw new RuntimeException("Failed to encrypt emergency contact phone", e);
        }
    }
}