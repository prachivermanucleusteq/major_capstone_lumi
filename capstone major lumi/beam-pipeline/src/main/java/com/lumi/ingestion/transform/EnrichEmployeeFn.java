package com.lumi.ingestion.transform;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.lumi.ingestion.model.EmployeeRecord;
import com.lumi.ingestion.util.EncryptionUtil;
import org.apache.beam.sdk.transforms.DoFn;

import java.time.Instant;
import java.util.UUID;

/* Adds pipeline-level metadata to each valid EmployeeRecord: */
public class EnrichEmployeeFn extends DoFn<EmployeeRecord, EmployeeRecord> {

    private final String executionId;
    private final String encryptionKey;

    public EnrichEmployeeFn(String executionId, String encryptionKey) {
        this.executionId   = executionId;
        this.encryptionKey = encryptionKey;
    }

    @ProcessElement
    public void processElement(@Element EmployeeRecord record, OutputReceiver<EmployeeRecord> out) {
        EmployeeRecord enriched = new EmployeeRecord(record, Instant.now(), UUID.fromString(executionId));
        com.lumi.ingestion.util.EncryptionUtil enc = new com.lumi.ingestion.util.EncryptionUtil(encryptionKey);
        if (record.getSalary() != null) {
            enriched.setSalaryEncrypted(enc.encrypt(record.getSalary().toPlainString()));
        }
        if (record.getPhoneNumber() != null) {
            enriched.setPhoneNumberEncrypted(enc.encrypt(record.getPhoneNumber()));
        }
        if (record.getEmergencyContact() != null ) {
            enriched.setEmergencyContactEncrypted(encryptEmergencyContactPhone(record.getEmergencyContact(), enc));
        }
        out.output(enriched);
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