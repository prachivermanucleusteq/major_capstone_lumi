package com.lumi.ingestion;

import com.lumi.ingestion.configuration.IngestionPipelineOptions;
import com.lumi.ingestion.error.ErrorRecord;
import com.lumi.ingestion.model.EmployeeRecord;
import com.lumi.ingestion.parser.CsvParser;
import com.lumi.ingestion.parser.JsonParser;
import com.lumi.ingestion.transform.EnrichEmployeeFn;
import com.lumi.ingestion.transform.ValidateEmployeeFn;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.beam.vendor.grpc.v1p60p1.com.google.api.client.json.Json;
import org.postgresql.util.PGobject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Array;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;

public class IngestionPipeline {

    private static final Logger LOG = LoggerFactory.getLogger(IngestionPipeline.class);

    static final String INSERT_SQL =
        "INSERT INTO employee ("
        + "employee_id, first_name, last_name, email, phone_number, hire_date, "
        + "department, job_title, salary, currency, employment_status, manager_id, "
        + "is_active, skills, address, emergency_contact, "
        + "ingestion_timestamp, execution_id, source_creation_time"
        + ") VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
        + " ON CONFLICT (employee_id) DO NOTHING";

    public static void main(String[] args) throws IOException {
        PipelineOptionsFactory.register(IngestionPipelineOptions.class);
        IngestionPipelineOptions options = PipelineOptionsFactory
                .fromArgs(args)
                .withValidation()
                .as(IngestionPipelineOptions.class);
        run(options);
    }

    public static void run(IngestionPipelineOptions options) throws IOException {
        String inputFile  = options.getInputFile();
        String executionId = options.getExecutionId();
        boolean isCsv = inputFile.toLowerCase().endsWith(".csv");
        Pipeline pipeline = Pipeline.create(options);
        PCollection<EmployeeRecord> parsedValid;
        PCollection<ErrorRecord>    parseErrors;

        if (isCsv) {
            // Read CSV: first line is the header; subsequent lines are data rows.
            List<String> lines = Files.readAllLines(Paths.get(inputFile));
            if (lines.isEmpty()) {
                throw new IllegalArgumentException("CSV file is empty: " + inputFile);
            }
            String header = lines.get(0);
            List<String> rows = new ArrayList<>();
            for (int i = 1; i < lines.size(); i++) {
                String row = lines.get(i);
                if (!row.isBlank()) {
                    rows.add(header + "\t" + row);
                }
            }
            PCollectionTuple parsed = pipeline
                    .apply("CreateCsvRows", Create.of(rows))
                    .apply("ParseCsv", ParDo
                            .of(new CsvParser(executionId, inputFile))
                            .withOutputTags(CsvParser.VALID_TAG,
                                    TupleTagList.of(CsvParser.ERROR_TAG)));
            parsedValid = parsed.get(CsvParser.VALID_TAG);
            parseErrors = parsed.get(CsvParser.ERROR_TAG);
        } else {
            // JSON: read the entire file as a single string (array or object).
            String rawJson = new String(Files.readAllBytes(Paths.get(inputFile)));
            PCollectionTuple parsed = pipeline
                    .apply("CreateInput", Create.of(rawJson))
                    .apply("ParseEmployee", ParDo
                            .of(new JsonParser (executionId, inputFile))
                            .withOutputTags(JsonParser.VALID_TAG,
                                    TupleTagList.of(JsonParser.ERROR_TAG)));
            parsedValid = parsed.get(JsonParser.VALID_TAG);
            parseErrors = parsed.get(JsonParser.ERROR_TAG);
        }

        // Validate 
        PCollectionTuple validated = parsedValid
                .apply("ValidateEmployee", ParDo
                        .of(new ValidateEmployeeFn(executionId, inputFile))
                        .withOutputTags(ValidateEmployeeFn.VALID_TAG,
                                TupleTagList.of(ValidateEmployeeFn.ERROR_TAG)));

        PCollection<EmployeeRecord> validRecords = validated.get(ValidateEmployeeFn.VALID_TAG);
        PCollection<ErrorRecord> validationErrors = validated.get(ValidateEmployeeFn.ERROR_TAG);

        // Enrich (includes encryption of salary + phone_number) 
        String encryptionKey = options.getEncryptionKey();
        PCollection<EmployeeRecord> enriched = validRecords
                .apply("EnrichEmployee", ParDo.of(new EnrichEmployeeFn(executionId, encryptionKey)));

        // Write to PostgreSQL 
        enriched.apply("WriteToPostgres", JdbcIO.<EmployeeRecord>write()
                .withDataSourceConfiguration(JdbcIO.DataSourceConfiguration
                        .create("org.postgresql.Driver", options.getJdbcUrl())
                        .withUsername(options.getJdbcUsername())
                        .withPassword(options.getJdbcPassword()))
                .withStatement(INSERT_SQL)
                .withPreparedStatementSetter((record, stmt) -> mapToStatement(record, stmt)));

        //  Write errors 
        PCollection<String> allErrors = parseErrors
                .apply("FormatParseErrors", MapElements.via(new SimpleFunction<ErrorRecord, String>() {
                    @Override public String apply(ErrorRecord e) { return e.toString(); }
                }));

        PCollection<String> allValidationErrors = validationErrors
                .apply("FormatValidationErrors", MapElements.via(new SimpleFunction<ErrorRecord, String>() {
                    @Override public String apply(ErrorRecord e) { return e.toString(); }
                }));

        // Flatten both error streams into one file
        PCollectionList.of(allErrors).and(allValidationErrors)
                .apply("FlattenErrors", Flatten.pCollections())
                .apply("WriteErrors", TextIO.write()
                        .to(options.getErrorOutput())
                        .withSuffix(".txt")
                        .withNumShards(1));

        pipeline.run().waitUntilFinish();
        LOG.info("Pipeline completed. executionId={} | valid records written to PostgreSQL" + " | error records (if any) written to {}.txt", executionId, options.getErrorOutput());
    }

    static void mapToStatement(EmployeeRecord r, PreparedStatement stmt) throws Exception {
        stmt.setObject(1, r.getEmployeeId(), Types.OTHER);
        stmt.setString(2, r.getFirstName());
        stmt.setString(3, r.getLastName());
        stmt.setString(4, r.getEmail());
        stmt.setString(5, r.getPhoneNumberEncrypted());
        if (r.getHireDate() != null) {
            stmt.setDate(6, Date.valueOf(r.getHireDate()));
        } else {
            stmt.setNull(6, Types.DATE);
        }
        stmt.setString(7, r.getDepartment());
        stmt.setString(8, r.getJobTitle());
        stmt.setString(9, r.getSalaryEncrypted());
        stmt.setString(10, r.getCurrency());
        stmt.setString(11, r.getEmploymentStatus());
        if (r.getManagerId() != null) {
            stmt.setObject(12, r.getManagerId(), Types.OTHER);
        } else {
            stmt.setNull(12, Types.OTHER);
        }
        if (r.getIsActive() != null) {
            stmt.setBoolean(13, r.getIsActive());
        } else {
            stmt.setNull(13, Types.BOOLEAN);
        }
        if (r.getSkills() != null && !r.getSkills().isEmpty()) {
            Array skillsArray = stmt.getConnection().createArrayOf(
                    "text", r.getSkills().toArray());
            stmt.setArray(14, skillsArray);
        } else {
            stmt.setNull(14, Types.ARRAY);
        }
        stmt.setObject(15, toPGobject("jsonb", r.getAddress()));
        stmt.setString(16, r.getEmergencyContactEncrypted());
        if (r.getIngestionTimestamp() != null) {
            stmt.setTimestamp(17, Timestamp.from(r.getIngestionTimestamp()));
        } else {
            stmt.setNull(17, Types.TIMESTAMP_WITH_TIMEZONE);
        }
        if (r.getExecutionId() != null) {
            stmt.setObject(18, r.getExecutionId(), Types.OTHER);
        } else {
            stmt.setNull(18, Types.OTHER);
        }
        if (r.getSourceCreationTime() != null) {
            stmt.setTimestamp(19, Timestamp.from(r.getSourceCreationTime()));
        } else {
            stmt.setNull(19, Types.TIMESTAMP_WITH_TIMEZONE);
        }

    }

    private static PGobject toPGobject(String type, String value) throws Exception {
        if (value == null) return null;
        PGobject obj = new PGobject();
        obj.setType(type);
        obj.setValue(value);
        return obj;
    }
}