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
import org.apache.beam.sdk.metrics.MetricNameFilter;
import org.apache.beam.sdk.metrics.MetricResult;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.metrics.MetricsFilter;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.*;
import org.apache.beam.sdk.PipelineResult;
import org.apache.commons.dbcp2.BasicDataSource;
import org.postgresql.util.PGobject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Array;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.Types;
import java.time.Instant;

public class IngestionPipeline {

    private static final Logger LOG = LoggerFactory.getLogger(IngestionPipeline.class);

    public static void main(String[] args) throws IOException {
        PipelineOptionsFactory.register(IngestionPipelineOptions.class);
        IngestionPipelineOptions options = PipelineOptionsFactory
                .fromArgs(args)
                .withValidation()
                .as(IngestionPipelineOptions.class);
        run(options);
    }

    public static void run(IngestionPipelineOptions options) throws IOException {
        String inputFile   = options.getInputFile();
        String executionId = options.getExecutionId();
        Instant sourceCreationTime = Instant.parse(options.getSourceCreationTime());
        boolean isCsv      = inputFile.toLowerCase().endsWith(".csv");

        Pipeline pipeline = Pipeline.create(options);
        PCollection<EmployeeRecord> parsedValid;
        PCollection<ErrorRecord>    parseErrors;

        // Parse
        if (isCsv) {
            String header;
            try (BufferedReader reader = Files.newBufferedReader(Paths.get(inputFile))) {
                header = reader.readLine();
            }
            if (header == null || header.isBlank()) {
                throw new IllegalArgumentException("CSV file is empty or has no header: " + inputFile);
            }
            final String csvHeader = header;
            PCollectionTuple parsed = pipeline
                    .apply("ReadCsvRows", TextIO.read().from(inputFile).withSkipHeaderLines(1))
                    .apply("RemoveBlankCsvRows", Filter.by(row -> !row.isBlank()))
                    .apply("PrefixCsvHeader", MapElements.into(TypeDescriptors.strings())
                            .via(row -> csvHeader + "\t" + row))
                    .apply("ParseCsv", ParDo
                            .of(new CsvParser(executionId, inputFile, sourceCreationTime))
                            .withOutputTags(CsvParser.VALID_TAG, TupleTagList.of(CsvParser.ERROR_TAG)));
            parsedValid = parsed.get(CsvParser.VALID_TAG);
            parseErrors = parsed.get(CsvParser.ERROR_TAG);
        } else {
            String jsonContent = new String(Files.readAllBytes(Paths.get(inputFile)), StandardCharsets.UTF_8).trim();
            PCollectionTuple parsed = pipeline
                    .apply("CreateJsonInput", org.apache.beam.sdk.transforms.Create.of(jsonContent))
                    .apply("ParseEmployee", ParDo
                            .of(new JsonParser(executionId, inputFile, sourceCreationTime))
                            .withOutputTags(JsonParser.VALID_TAG, TupleTagList.of(JsonParser.ERROR_TAG)));
            parsedValid = parsed.get(JsonParser.VALID_TAG);
            parseErrors = parsed.get(JsonParser.ERROR_TAG);
        }

        // Validate
        PCollectionTuple validated = parsedValid
                .apply("ValidateEmployee", ParDo
                        .of(new ValidateEmployeeFn(executionId, inputFile))
                        .withOutputTags(ValidateEmployeeFn.VALID_TAG, TupleTagList.of(ValidateEmployeeFn.ERROR_TAG)));

        PCollection<EmployeeRecord> validRecords     = validated.get(ValidateEmployeeFn.VALID_TAG);
        PCollection<ErrorRecord>    validationErrors  = validated.get(ValidateEmployeeFn.ERROR_TAG);

        // Enrich and Encrypt
        PCollectionTuple enrichedTuple = validRecords
                .apply("EnrichEmployee", ParDo
                        .of(new EnrichEmployeeFn(executionId, inputFile, options.getEncryptionKey()))
                        .withOutputTags(EnrichEmployeeFn.VALID_TAG, TupleTagList.of(EnrichEmployeeFn.ERROR_TAG)));

        PCollection<EmployeeRecord> enriched    = enrichedTuple.get(EnrichEmployeeFn.VALID_TAG);
        PCollection<ErrorRecord>    enrichErrors = enrichedTuple.get(EnrichEmployeeFn.ERROR_TAG);

        // Write to PostgreSQL
        PCollectionTuple writeResults = enriched
                .apply("WriteToPostgres", ParDo
                        .of(new JdbcWriteFn(executionId,
                                inputFile,
                                options.getJdbcUrl(),
                                options.getJdbcUsername(),
                                options.getJdbcPassword()))
                        .withOutputTags(JdbcWriteFn.SUCCESS_TAG, TupleTagList.of(JdbcWriteFn.ERROR_TAG)));

        PCollection<ErrorRecord> dbErrors = writeResults.get(JdbcWriteFn.ERROR_TAG);

        // Write all errors to one file
        PCollectionList.of(toStrings(parseErrors,       "FormatParseErrors"))
                .and(toStrings(validationErrors,  "FormatValidationErrors"))
                .and(toStrings(enrichErrors,      "FormatEnrichErrors"))
                .and(toStrings(dbErrors,          "FormatDbErrors"))
                .apply("FlattenErrors", Flatten.pCollections())
                .apply("WriteErrors", TextIO.write()
                        .to(options.getErrorOutput())
                        .withSuffix(".txt")
                        .withNumShards(1));

        PipelineResult result = pipeline.run();
        result.waitUntilFinish();
        Iterable<MetricResult<Long>> databaseWriteCounters = result.metrics().queryMetrics(
                        MetricsFilter.builder()
                                .addNameFilter(MetricNameFilter.named("lumi", "db_write_success"))
                                .build())
                .getCounters();
        long successfulDatabaseOperations = 0;
        for (MetricResult<Long> counter : databaseWriteCounters) {
            Long attempted = counter.getAttempted();
            if (attempted != null) {
                successfulDatabaseOperations += attempted;
            }
        }
        System.out.println("LUMI_DB_WRITE_SUCCESS_COUNT=" + successfulDatabaseOperations);
        LOG.info("Pipeline completed. executionId={} | errors written to {}.txt", executionId, options.getErrorOutput());
    }

    private static PCollection<String> toStrings(PCollection<ErrorRecord> errors, String stepName) {
        return errors.apply(stepName, MapElements.via(new SimpleFunction<ErrorRecord, String>() {
            @Override public String apply(ErrorRecord e) { return e.toString(); }
        }));
    }

    // Inner DoFn: JDBC write with per-record error routing
    static final class JdbcWriteFn extends DoFn<EmployeeRecord, Void> {

        static final TupleTag<Void> SUCCESS_TAG = new TupleTag<>() {};
        static final TupleTag<ErrorRecord> ERROR_TAG   = new TupleTag<>() {};
        private static final org.apache.beam.sdk.metrics.Counter DB_WRITE_SUCCESS_COUNTER =
                Metrics.counter("lumi", "db_write_success");

        private static final String INSERT_SQL =
                "INSERT INTO employee ("
                        + "employee_id, first_name, last_name, email, phone_number, hire_date, "
                        + "department, job_title, salary, currency, employment_status, manager_id, "
                        + "is_active, skills, address, emergency_contact, "
                        + "ingestion_timestamp, execution_id, source_creation_time"
                        + ") VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
                        + " ON CONFLICT (employee_id) DO UPDATE SET "
                        + "first_name = EXCLUDED.first_name, "
                        + "last_name = EXCLUDED.last_name, "
                        + "email = EXCLUDED.email, "
                        + "phone_number = EXCLUDED.phone_number, "
                        + "hire_date = EXCLUDED.hire_date, "
                        + "department = EXCLUDED.department, "
                        + "job_title = EXCLUDED.job_title, "
                        + "salary = EXCLUDED.salary, "
                        + "currency = EXCLUDED.currency, "
                        + "employment_status = EXCLUDED.employment_status, "
                        + "manager_id = EXCLUDED.manager_id, "
                        + "is_active = EXCLUDED.is_active, "
                        + "skills = EXCLUDED.skills, "
                        + "address = EXCLUDED.address, "
                        + "emergency_contact = EXCLUDED.emergency_contact, "
                        + "ingestion_timestamp = EXCLUDED.ingestion_timestamp, "
                        + "execution_id = EXCLUDED.execution_id, "
                        + "source_creation_time = EXCLUDED.source_creation_time";

        private final String executionId;
        private final String sourceFile;
        private final String jdbcUrl;
        private final String jdbcUsername;
        private final String jdbcPassword;

        private transient BasicDataSource dataSource;

        JdbcWriteFn(String executionId, String sourceFile, String jdbcUrl, String jdbcUsername, String jdbcPassword) {
            this.executionId  = executionId;
            this.sourceFile = sourceFile;
            this.jdbcUrl      = jdbcUrl;
            this.jdbcUsername = jdbcUsername;
            this.jdbcPassword = jdbcPassword;
        }

        @Setup
        public void setup() {
            dataSource = new BasicDataSource();
            dataSource.setDriverClassName("org.postgresql.Driver");
            dataSource.setUrl(jdbcUrl);
            dataSource.setUsername(jdbcUsername);
            dataSource.setPassword(jdbcPassword);
            dataSource.setMaxTotal(4);
        }

        @Teardown
        public void teardown() throws Exception {
            if (dataSource != null) dataSource.close();
        }

        @ProcessElement
        public void processElement(@Element EmployeeRecord r, MultiOutputReceiver out) {
            try (Connection conn = dataSource.getConnection();
                 PreparedStatement stmt = conn.prepareStatement(INSERT_SQL)) {

                stmt.setObject(1, r.getEmployeeId(), Types.OTHER);
                stmt.setString(2, r.getFirstName());
                stmt.setString(3, r.getLastName());
                stmt.setString(4, r.getEmail());
                stmt.setString(5, r.getPhoneNumberEncrypted());
                if (r.getHireDate() != null) stmt.setDate(6, Date.valueOf(r.getHireDate()));
                else stmt.setNull(6, Types.DATE);
                stmt.setString(7, r.getDepartment());
                stmt.setString(8, r.getJobTitle());
                stmt.setString(9, r.getSalaryEncrypted());
                stmt.setString(10, r.getCurrency());
                stmt.setString(11, r.getEmploymentStatus());
                if (r.getManagerId() != null) stmt.setObject(12, r.getManagerId(), Types.OTHER);
                else stmt.setNull(12, Types.OTHER);
                if (r.getIsActive() != null) stmt.setBoolean(13, r.getIsActive());
                else stmt.setNull(13, Types.BOOLEAN);
                if (r.getSkills() != null && !r.getSkills().isEmpty()) {
                    Array arr = conn.createArrayOf("text", r.getSkills().toArray());
                    stmt.setArray(14, arr);
                } else {
                    stmt.setNull(14, Types.ARRAY);
                }
                stmt.setObject(15, toPGobject("jsonb", r.getAddress()));
                stmt.setObject(16, toPGobject("jsonb", r.getEmergencyContactEncrypted()));
                if (r.getIngestionTimestamp() != null) stmt.setTimestamp(17, java.sql.Timestamp.from(r.getIngestionTimestamp()));
                else stmt.setNull(17, Types.TIMESTAMP_WITH_TIMEZONE);
                if (r.getExecutionId() != null) stmt.setObject(18, r.getExecutionId(), Types.OTHER);
                else stmt.setNull(18, Types.OTHER);
                if (r.getSourceCreationTime() != null) stmt.setTimestamp(19, java.sql.Timestamp.from(r.getSourceCreationTime()));
                else stmt.setNull(19, Types.TIMESTAMP_WITH_TIMEZONE);

                int affectedRows = stmt.executeUpdate();
                DB_WRITE_SUCCESS_COUNTER.inc(affectedRows);

            } catch (Exception e) {
                String recordId = r.getEmployeeId() != null ? r.getEmployeeId().toString() : "unknown";
                LOG.warn("DB write failed for record {}: {}", recordId, e.getMessage());
                out.get(ERROR_TAG).output(new ErrorRecord(executionId, sourceFile,
                        "DB write failed: " + e.getMessage(), Instant.now()));
            }
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