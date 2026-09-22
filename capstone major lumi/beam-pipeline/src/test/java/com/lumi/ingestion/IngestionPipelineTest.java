package com.lumi.ingestion;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.lumi.ingestion.error.ErrorRecord;
import com.lumi.ingestion.model.EmployeeRecord;
import com.lumi.ingestion.transform.EnrichEmployeeFn;
import com.lumi.ingestion.transform.ParseEmployeeFn;
import com.lumi.ingestion.transform.ValidateEmployeeFn;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTagList;
import org.junit.Rule;
import org.junit.Test;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.util.UUID;

import static org.junit.Assert.*;

public class IngestionPipelineTest {

   private static final String EXEC_ID = "00000000-0000-0000-0000-000000000001";
   private static final String SOURCE = "test-source.json";
        private static final String ENCRYPTION_KEY = "01234567890123456789012345678901";

   @Rule
   public final TestPipeline pipeline = TestPipeline.create();

   // -------------------------------------------------------------------------
   // 1. Valid JSON parsing
   // -------------------------------------------------------------------------
   @Test
   public void testValidJsonParsing() {
       String json = validJson();
       PCollectionTuple result = pipeline
               .apply(Create.of(json))
               .apply(ParDo.of(new ParseEmployeeFn(EXEC_ID, SOURCE))
                       .withOutputTags(ParseEmployeeFn.VALID_TAG,
                               TupleTagList.of(ParseEmployeeFn.ERROR_TAG)));

       PAssert.that(result.get(ParseEmployeeFn.VALID_TAG)).satisfies(records -> {
           EmployeeRecord r = records.iterator().next();
           assertEquals(UUID.fromString("550e8400-e29b-41d4-a716-446655440000"), r.getEmployeeId());
           assertEquals("John", r.getFirstName());
           assertEquals("john.smith@example.com", r.getEmail());
           return null;
       });
       PAssert.that(result.get(ParseEmployeeFn.ERROR_TAG)).empty();
       pipeline.run().waitUntilFinish();
   }

   // -------------------------------------------------------------------------
   // 2. Malformed JSON goes to error output
   // -------------------------------------------------------------------------
   @Test
   public void testMalformedJsonRoutedToError() {
       String badJson = "{this is not json";
       PCollectionTuple result = pipeline
               .apply(Create.of(badJson))
               .apply(ParDo.of(new ParseEmployeeFn(EXEC_ID, SOURCE))
                       .withOutputTags(ParseEmployeeFn.VALID_TAG,
                               TupleTagList.of(ParseEmployeeFn.ERROR_TAG)));

       PAssert.that(result.get(ParseEmployeeFn.VALID_TAG)).empty();
       PAssert.that(result.get(ParseEmployeeFn.ERROR_TAG)).satisfies(errors -> {
           ErrorRecord e = errors.iterator().next();
           assertEquals(EXEC_ID, e.getExecutionId());
           assertNotNull(e.getErrorReason());
           return null;
       });
       pipeline.run().waitUntilFinish();
   }

   // -------------------------------------------------------------------------
   // 3. UUID parsing — valid and invalid
   // -------------------------------------------------------------------------
   @Test
   public void testInvalidUuidRoutedToError() {
       String json = validJson().replace(
               "550e8400-e29b-41d4-a716-446655440000", "NOT-A-UUID");
       PCollectionTuple result = pipeline
               .apply(Create.of(json))
               .apply(ParDo.of(new ParseEmployeeFn(EXEC_ID, SOURCE))
                       .withOutputTags(ParseEmployeeFn.VALID_TAG,
                               TupleTagList.of(ParseEmployeeFn.ERROR_TAG)));

       PAssert.that(result.get(ParseEmployeeFn.VALID_TAG)).empty();
       PAssert.that(result.get(ParseEmployeeFn.ERROR_TAG)).satisfies(errors -> {
           assertTrue(errors.iterator().hasNext());
           return null;
       });
       pipeline.run().waitUntilFinish();
   }

   // -------------------------------------------------------------------------
   // 4. Date parsing
   // -------------------------------------------------------------------------
   @Test
   public void testDateParsing() {
       String json = validJson();
       PCollectionTuple result = pipeline
               .apply(Create.of(json))
               .apply(ParDo.of(new ParseEmployeeFn(EXEC_ID, SOURCE))
                       .withOutputTags(ParseEmployeeFn.VALID_TAG,
                               TupleTagList.of(ParseEmployeeFn.ERROR_TAG)));

       PAssert.that(result.get(ParseEmployeeFn.VALID_TAG)).satisfies(records -> {
           assertEquals(LocalDate.of(2024, 1, 15),
                   records.iterator().next().getHireDate());
           return null;
       });
       pipeline.run().waitUntilFinish();
   }

   // -------------------------------------------------------------------------
   // 5. BigDecimal salary parsing
   // -------------------------------------------------------------------------
   @Test
   public void testSalaryParsedAsBigDecimal() {
       String json = validJson();
       PCollectionTuple result = pipeline
               .apply(Create.of(json))
               .apply(ParDo.of(new ParseEmployeeFn(EXEC_ID, SOURCE))
                       .withOutputTags(ParseEmployeeFn.VALID_TAG,
                               TupleTagList.of(ParseEmployeeFn.ERROR_TAG)));

       PAssert.that(result.get(ParseEmployeeFn.VALID_TAG)).satisfies(records -> {
           BigDecimal salary = records.iterator().next().getSalary();
           assertEquals(0, new BigDecimal("85000.00").compareTo(salary));
           return null;
       });
       pipeline.run().waitUntilFinish();
   }

   // -------------------------------------------------------------------------
   // 6. Null/missing fields apply the type-safe policy
   // -------------------------------------------------------------------------
   @Test
   public void testNullFieldsApplyPolicy() {
       // manager_id is null in the valid JSON — should remain null (UUID policy)
       // skills is present; test with missing department
       String json = validJson().replace("\"department\":\"Technology\",", "");
       PCollectionTuple result = pipeline
               .apply(Create.of(json))
               .apply(ParDo.of(new ParseEmployeeFn(EXEC_ID, SOURCE))
                       .withOutputTags(ParseEmployeeFn.VALID_TAG,
                               TupleTagList.of(ParseEmployeeFn.ERROR_TAG)));

       PAssert.that(result.get(ParseEmployeeFn.VALID_TAG)).satisfies(records -> {
           EmployeeRecord r = records.iterator().next();
           assertNull(r.getManagerId());           // UUID null policy
           assertEquals(" ", r.getDepartment());   // String whitespace policy
           return null;
       });
       pipeline.run().waitUntilFinish();
   }

   // -------------------------------------------------------------------------
   // 7. Invalid field lengths caught by ValidateEmployeeFn
   // -------------------------------------------------------------------------
   @Test
   public void testInvalidFieldLengthRoutedToError() {
       // first_name "Jo" has length 2, violates CHECK (BETWEEN 3 AND 15)
       String json = validJson().replace("\"first_name\":\"John\"", "\"first_name\":\"Jo\"");
       PCollectionTuple parsed = pipeline
               .apply(Create.of(json))
               .apply(ParDo.of(new ParseEmployeeFn(EXEC_ID, SOURCE))
                       .withOutputTags(ParseEmployeeFn.VALID_TAG,
                               TupleTagList.of(ParseEmployeeFn.ERROR_TAG)));

       PCollectionTuple validated = parsed.get(ParseEmployeeFn.VALID_TAG)
               .apply(ParDo.of(new ValidateEmployeeFn(EXEC_ID, SOURCE))
                       .withOutputTags(ValidateEmployeeFn.VALID_TAG,
                               TupleTagList.of(ValidateEmployeeFn.ERROR_TAG)));

       PAssert.that(validated.get(ValidateEmployeeFn.VALID_TAG)).empty();
       PAssert.that(validated.get(ValidateEmployeeFn.ERROR_TAG)).satisfies(errors -> {
           ErrorRecord e = errors.iterator().next();
           assertTrue(e.getErrorReason().contains("first_name"));
           return null;
       });
       pipeline.run().waitUntilFinish();
   }

   // -------------------------------------------------------------------------
   // 8. Valid employee passes parse + validate
   // -------------------------------------------------------------------------
   @Test
   public void testValidEmployeePassesParseThenValidate() {
       PCollectionTuple parsed = pipeline
               .apply(Create.of(validJson()))
               .apply(ParDo.of(new ParseEmployeeFn(EXEC_ID, SOURCE))
                       .withOutputTags(ParseEmployeeFn.VALID_TAG,
                               TupleTagList.of(ParseEmployeeFn.ERROR_TAG)));

       PCollectionTuple validated = parsed.get(ParseEmployeeFn.VALID_TAG)
               .apply(ParDo.of(new ValidateEmployeeFn(EXEC_ID, SOURCE))
                       .withOutputTags(ValidateEmployeeFn.VALID_TAG,
                               TupleTagList.of(ValidateEmployeeFn.ERROR_TAG)));

       PAssert.that(validated.get(ValidateEmployeeFn.VALID_TAG)).satisfies(records -> {
           assertTrue(records.iterator().hasNext());
           return null;
       });
       PAssert.that(validated.get(ValidateEmployeeFn.ERROR_TAG)).empty();
       pipeline.run().waitUntilFinish();
   }

   // -------------------------------------------------------------------------
   // 9. Invalid record is routed to error, does not reach valid output
   // -------------------------------------------------------------------------
   @Test
   public void testInvalidRecordDoesNotReachValidOutput() {
       // null employee_id — parse succeeds but validate rejects
       String json = validJson().replace(
               "\"employee_id\":\"550e8400-e29b-41d4-a716-446655440000\"",
               "\"employee_id\":null");
       PCollectionTuple parsed = pipeline
               .apply(Create.of(json))
               .apply(ParDo.of(new ParseEmployeeFn(EXEC_ID, SOURCE))
                       .withOutputTags(ParseEmployeeFn.VALID_TAG,
                               TupleTagList.of(ParseEmployeeFn.ERROR_TAG)));

       PCollectionTuple validated = parsed.get(ParseEmployeeFn.VALID_TAG)
               .apply(ParDo.of(new ValidateEmployeeFn(EXEC_ID, SOURCE))
                       .withOutputTags(ValidateEmployeeFn.VALID_TAG,
                               TupleTagList.of(ValidateEmployeeFn.ERROR_TAG)));

       PAssert.that(validated.get(ValidateEmployeeFn.VALID_TAG)).empty();
       PAssert.that(validated.get(ValidateEmployeeFn.ERROR_TAG)).satisfies(errors -> {
           assertTrue(errors.iterator().hasNext());
           return null;
       });
       pipeline.run().waitUntilFinish();
   }

   // -------------------------------------------------------------------------
   // 10. Metadata enrichment sets all three metadata fields
   // -------------------------------------------------------------------------
   @Test
   public void testMetadataEnrichment() {
       PCollectionTuple parsed = pipeline
               .apply(Create.of(validJson()))
               .apply(ParDo.of(new ParseEmployeeFn(EXEC_ID, SOURCE))
                       .withOutputTags(ParseEmployeeFn.VALID_TAG,
                               TupleTagList.of(ParseEmployeeFn.ERROR_TAG)));

       PCollectionTuple validated = parsed.get(ParseEmployeeFn.VALID_TAG)
               .apply(ParDo.of(new ValidateEmployeeFn(EXEC_ID, SOURCE))
                       .withOutputTags(ValidateEmployeeFn.VALID_TAG,
                               TupleTagList.of(ValidateEmployeeFn.ERROR_TAG)));

       PAssert.that(
               validated.get(ValidateEmployeeFn.VALID_TAG)
                       .apply(ParDo.of(new EnrichEmployeeFn(EXEC_ID, ENCRYPTION_KEY)))
       ).satisfies(records -> {
           EmployeeRecord r = records.iterator().next();
           assertNotNull("ingestion_timestamp must be set", r.getIngestionTimestamp());
           assertEquals(UUID.fromString(EXEC_ID), r.getExecutionId());
           assertNotNull("source_creation_time must be set", r.getSourceCreationTime());
           return null;
       });
       pipeline.run().waitUntilFinish();
   }

   // -------------------------------------------------------------------------
   // 11. EnrichEmployeeFn output is a distinct object — input is not mutated
   // -------------------------------------------------------------------------
   @Test
   public void testEnrichDoesNotMutateInput() {
       // Run parse → validate → enrich through TestPipeline and confirm
       // the enriched record carries metadata while source fields are preserved.
       PCollectionTuple parsed = pipeline
               .apply(Create.of(validJson()))
               .apply(ParDo.of(new ParseEmployeeFn(EXEC_ID, SOURCE))
                       .withOutputTags(ParseEmployeeFn.VALID_TAG,
                               TupleTagList.of(ParseEmployeeFn.ERROR_TAG)));

       PCollectionTuple validated = parsed.get(ParseEmployeeFn.VALID_TAG)
               .apply(ParDo.of(new ValidateEmployeeFn(EXEC_ID, SOURCE))
                       .withOutputTags(ValidateEmployeeFn.VALID_TAG,
                               TupleTagList.of(ValidateEmployeeFn.ERROR_TAG)));

       PAssert.that(
               validated.get(ValidateEmployeeFn.VALID_TAG)
                       .apply(ParDo.of(new EnrichEmployeeFn(EXEC_ID, ENCRYPTION_KEY)))
       ).satisfies(records -> {
           EmployeeRecord r = records.iterator().next();
           // source fields must be preserved
           assertEquals(UUID.fromString("550e8400-e29b-41d4-a716-446655440000"), r.getEmployeeId());
           assertEquals("John", r.getFirstName());
           // metadata fields must be populated by EnrichEmployeeFn
           assertNotNull(r.getIngestionTimestamp());
           assertEquals(UUID.fromString(EXEC_ID), r.getExecutionId());
           assertNotNull(r.getSourceCreationTime());
           return null;
       });
       pipeline.run().waitUntilFinish();
   }

   @Test
   public void testOnlyEmergencyContactPhoneIsEncrypted() {
       PCollectionTuple parsed = pipeline
               .apply(Create.of(validJson()))
               .apply(ParDo.of(new ParseEmployeeFn(EXEC_ID, SOURCE))
                       .withOutputTags(ParseEmployeeFn.VALID_TAG,
                               TupleTagList.of(ParseEmployeeFn.ERROR_TAG)));

       PAssert.that(parsed.get(ParseEmployeeFn.VALID_TAG)
               .apply(ParDo.of(new EnrichEmployeeFn(EXEC_ID, ENCRYPTION_KEY))))
               .satisfies(records -> {
                   EmployeeRecord record = records.iterator().next();
                   JsonNode emergency = new ObjectMapper()
                           .readTree(record.getEmergencyContactEncrypted());

                   assertEquals("Jane Smith", emergency.get("name").asText());
                   assertEquals("Spouse", emergency.get("relationship").asText());
                   assertEquals("9876543211", new com.lumi.ingestion.util.EncryptionUtil(ENCRYPTION_KEY)
                           .decrypt(emergency.get("phone").asText()));
                   return null;
               });
       pipeline.run().waitUntilFinish();
   }

   // 12. Mixed input: valid record reaches valid output, bad record reaches error output
   @Test
   public void testValidAndInvalidRecordsRoutedIndependently() {
       String validRecord   = validJson();
       String invalidRecord = "{this is not json}";

       PCollectionTuple parsedValid = pipeline
               .apply("ValidInput", Create.of(validRecord))
               .apply("ParseValid", ParDo.of(new ParseEmployeeFn(EXEC_ID, SOURCE))
                       .withOutputTags(ParseEmployeeFn.VALID_TAG,
                               TupleTagList.of(ParseEmployeeFn.ERROR_TAG)));

       PCollectionTuple parsedInvalid = pipeline
               .apply("InvalidInput", Create.of(invalidRecord))
               .apply("ParseInvalid", ParDo.of(new ParseEmployeeFn(EXEC_ID, SOURCE))
                       .withOutputTags(ParseEmployeeFn.VALID_TAG,
                               TupleTagList.of(ParseEmployeeFn.ERROR_TAG)));

       // Valid record must reach the valid output
       PAssert.that(parsedValid.get(ParseEmployeeFn.VALID_TAG)).satisfies(records -> {
           assertTrue("valid record must reach valid output", records.iterator().hasNext());
           return null;
       });
       PAssert.that(parsedValid.get(ParseEmployeeFn.ERROR_TAG)).empty();

       // Invalid record must reach the error output with a reason
       PAssert.that(parsedInvalid.get(ParseEmployeeFn.VALID_TAG)).empty();
       PAssert.that(parsedInvalid.get(ParseEmployeeFn.ERROR_TAG)).satisfies(errors -> {
           ErrorRecord e = errors.iterator().next();
           assertNotNull("error reason must be present", e.getErrorReason());
           assertEquals(EXEC_ID, e.getExecutionId());
           assertEquals(SOURCE,  e.getSourceFile());
           return null;
       });

       pipeline.run().waitUntilFinish();
   }

   // 13. JSON array input: each element parsed as a separate EmployeeRecord
   @Test
   public void testJsonArrayInputParsesEachElementSeparately() {
       String arrayJson =
               "[" + validJson() + ","
               + validJson().replace(
                       "550e8400-e29b-41d4-a716-446655440000",
                       "550e8400-e29b-41d4-a716-446655440099")
               + "]";

       PCollectionTuple result = pipeline
               .apply(Create.of(arrayJson))
               .apply(ParDo.of(new ParseEmployeeFn(EXEC_ID, SOURCE))
                       .withOutputTags(ParseEmployeeFn.VALID_TAG,
                               TupleTagList.of(ParseEmployeeFn.ERROR_TAG)));

       PAssert.that(result.get(ParseEmployeeFn.VALID_TAG)).satisfies(records -> {
           long count = 0;
           for (EmployeeRecord ignored : records) count++;
           assertEquals("array of 2 should produce 2 valid records", 2, count);
           return null;
       });
       PAssert.that(result.get(ParseEmployeeFn.ERROR_TAG)).empty();
       pipeline.run().waitUntilFinish();
   }

   // -------------------------------------------------------------------------
   // 14. JSON array: one bad element goes to error, valid elements still processed
   // -------------------------------------------------------------------------
   @Test
   public void testJsonArrayBadElementIsolated() {
       String goodElement = validJson();
       String badElement  = validJson().replace(
               "550e8400-e29b-41d4-a716-446655440000", "NOT-A-UUID");
       String arrayJson = "[" + goodElement + "," + badElement + "]";

       PCollectionTuple result = pipeline
               .apply(Create.of(arrayJson))
               .apply(ParDo.of(new ParseEmployeeFn(EXEC_ID, SOURCE))
                       .withOutputTags(ParseEmployeeFn.VALID_TAG,
                               TupleTagList.of(ParseEmployeeFn.ERROR_TAG)));

       PAssert.that(result.get(ParseEmployeeFn.VALID_TAG)).satisfies(records -> {
           assertTrue("valid element must reach valid output", records.iterator().hasNext());
           return null;
       });
       PAssert.that(result.get(ParseEmployeeFn.ERROR_TAG)).satisfies(errors -> {
           ErrorRecord e = errors.iterator().next();
           assertNotNull(e.getErrorReason());
           assertEquals(EXEC_ID, e.getExecutionId());
           return null;
       });
       pipeline.run().waitUntilFinish();
   }

   // Helper
   private String validJson() {
       return "{\"employee_id\":\"550e8400-e29b-41d4-a716-446655440000\","
            + "\"first_name\":\"John\","
            + "\"last_name\":\"Smith\","
            + "\"email\":\"john.smith@example.com\","
            + "\"phone_number\":\"9876543210\","
            + "\"hire_date\":\"2024-01-15\","
            + "\"department\":\"Technology\","
            + "\"job_title\":\"Software Engineer\","
            + "\"salary\":85000.00,"
            + "\"currency\":\"USD\","
            + "\"employment_status\":\"ACTIVE\","
            + "\"manager_id\":null,"
            + "\"is_active\":true,"
            + "\"skills\":[\"Java\",\"Spring Boot\",\"Apache Beam\"],"
            + "\"address\":{\"city\":\"Indore\",\"state\":\"Madhya Pradesh\",\"country\":\"India\"},"
            + "\"emergency_contact\":{\"name\":\"Jane Smith\",\"relationship\":\"Spouse\",\"phone\":\"9876543211\"}"
            + "}";
   }
}