package com.lumi.ingestion.service;

import com.lumi.ingestion.client.airflow.AirflowClient;
import com.lumi.ingestion.dto.request.TriggerRequest;
import com.lumi.ingestion.dto.response.TriggerResponse;
import com.lumi.ingestion.exception.IngestionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

@Service
public class IngestionService {

    private static final Logger LOG = LoggerFactory.getLogger(IngestionService.class);

    private static final String DAG_RUN_PREFIX = "lumi-";
    private static final String SPLIT_OUTPUT_PREFIX = "SPLIT_OUTPUT:";
    private final AirflowClient airflowClient;
    private final String dagId;
    private final String jdbcUrl;
    private final String jdbcUsername;
    private final String jdbcPassword;
    private final String errorOutputBase;
    private final long splitThresholdKb;
    private final String splitOutputBase;
    private final String sparkSubmit;
    private final String pysparkScript;

    public IngestionService(
            AirflowClient airflowClient,
            @Value("${airflow.dag-id}") String dagId,
            @Value("${pipeline.jdbc-url}") String jdbcUrl,
            @Value("${pipeline.jdbc-username}") String jdbcUsername,
            @Value("${pipeline.jdbc-password}") String jdbcPassword,
            @Value("${file.error-output-base}") String errorOutputBase,
            @Value("${file.split-threshold-kb}") long splitThresholdKb,
            @Value("${file.split-output-base}") String splitOutputBase,
            @Value("${file.spark-submit}") String sparkSubmit,
            @Value("${file.pyspark-script}") String pysparkScript) {
        this.airflowClient = airflowClient;
        this.dagId = dagId;
        this.jdbcUrl = jdbcUrl;
        this.jdbcUsername = jdbcUsername;
        this.jdbcPassword = jdbcPassword;
        this.errorOutputBase = errorOutputBase;
        this.splitThresholdKb = splitThresholdKb;
        this.splitOutputBase = splitOutputBase;
        this.sparkSubmit = sparkSubmit;
        this.pysparkScript = pysparkScript;
    }

    // Starts the ingestion workflow: One API request generates exactly one executionId and the same executionId is passed to every DAG run.*/
    public TriggerResponse trigger(TriggerRequest request) {
        String executionId = UUID.randomUUID().toString();
        LOG.info("Starting ingestion: executionId={}, controlFile={}", executionId, request.getControlFile());

        // Read and validate control file
        Properties controlProperties = readControlFile(request.getControlFile());
        String fileName = getRequiredProperty(controlProperties, "file_name");
        String filePath = getRequiredProperty(controlProperties, "file_path");
        String recordCount = getRequiredProperty(controlProperties, "record_count");
        validateRecordCount(recordCount);

        LOG.info("Control file loaded: executionId={}, fileName={}, expectedRecordCount={}", executionId, fileName, recordCount);

        Path inputFile = resolveInputFile(filePath);

        // Check file size before starting Spark
        long fileSizeBytes = getFileSize(inputFile);
        long thresholdBytes = splitThresholdKb * 1024;
        LOG.info("Input file size checked: executionId={}, file={}, sizeBytes={}, thresholdKb={}", executionId, inputFile, fileSizeBytes, splitThresholdKb);

        // Decide whether the file needs splitting
        List<String> inputFiles;
        if (fileSizeBytes > thresholdBytes) {
            LOG.info("File exceeds split threshold. Starting Spark splitter: executionId={}", executionId);
            Path outputDirectory = Paths.get(splitOutputBase, executionId);
            inputFiles = invokeSparkSplitter(inputFile, outputDirectory);
            if (inputFiles.isEmpty()) {
                throw new IngestionException("Spark splitter produced no output files");
            }
            LOG.info("Spark splitting completed: executionId={}, splitFiles={}", executionId, inputFiles.size());
        } else {
            inputFiles = List.of(inputFile.toString());
            LOG.info("File is within split threshold. No splitting required: executionId={}", executionId);
        }

        // Trigger one Airflow DAG run for every input file
        List<String> dagRunIds = triggerDagRuns(executionId, inputFiles);
        LOG.info("Ingestion request accepted: executionId={}, inputFiles={}, dagRuns={}", executionId, inputFiles.size(), dagRunIds.size());
        return new TriggerResponse(executionId, dagRunIds, inputFiles, "ACCEPTED");
    }

    // Triggers one Airflow DAG run for each input file
    private List<String> triggerDagRuns(String executionId, List<String> inputFiles) {

        List<String> dagRunIds = new ArrayList<>();
        for (int i = 0; i < inputFiles.size(); i++) {
            String inputFile = inputFiles.get(i);
            String partSuffix = inputFiles.size() > 1 ? "-part-" + String.format("%03d", i + 1) : "";
            String dagRunId = DAG_RUN_PREFIX + executionId + partSuffix;
            String errorOutput = buildErrorOutputPath(executionId, i, inputFiles.size());
            Map<String, String> conf = new HashMap<>();
            conf.put("input_file", inputFile);
            conf.put("execution_id", executionId);
            conf.put("jdbc_url", jdbcUrl);
            conf.put("jdbc_username", jdbcUsername);
            conf.put("jdbc_password", jdbcPassword);
            conf.put("error_output", errorOutput);
            LOG.info("Triggering Airflow DAG: executionId={}, dagRunId={}, inputFile={}", executionId, dagRunId, inputFile
            );
            String confirmedRunId = airflowClient.triggerDagRun(dagId, dagRunId, conf);
            dagRunIds.add(confirmedRunId);
        }

        return dagRunIds;
    }

    // Builds the error output location for a DAG run
    private String buildErrorOutputPath(String executionId, int index, int totalFiles) {
        if (totalFiles == 1) {
            return Paths.get(errorOutputBase, executionId, "errors").toString();
        }
        return Paths.get(errorOutputBase, executionId, "part-" + String.format("%03d", index + 1), "errors").toString();
    }

    // Reads the control file
    private Properties readControlFile(String controlFilePath) {
        Path path;
        try {
            path = Paths.get(controlFilePath).toAbsolutePath().normalize();
        } catch (Exception ex) {
            throw new IngestionException("Invalid control file path: " + controlFilePath, ex);
        }

        if (!Files.exists(path)) {
            throw new IngestionException("Control file not found: " + path);
        }

        if (!Files.isRegularFile(path)) {
            throw new IngestionException("Control file is not a regular file: " + path);
        }

        Properties properties = new Properties();
        try (InputStream inputStream = Files.newInputStream(path)) {
            properties.load(inputStream);
            return properties;
        } catch (IOException ex) {
            LOG.error("Failed to read control file: {}", path, ex);
            throw new IngestionException("Failed to read control file: " + path, ex);
        }
    }

    // Gets a mandatory property from the control file
    private String getRequiredProperty(Properties properties, String propertyName) {
        String value = properties.getProperty(propertyName);
        if (value == null || value.isBlank()) {
            throw new IngestionException(propertyName + " is missing in control file");
        }
        return value.trim();
    }

    // Validates record_count from the control file
    private void validateRecordCount(String recordCount) {
        try {
            long count = Long.parseLong(recordCount);
            if (count < 0) {
                throw new IngestionException("record_count cannot be negative");
            }

        } catch (NumberFormatException ex) {
            throw new IngestionException("record_count must be a valid number", ex);
        }
    }

    // Resolves and validates the input file path
    private Path resolveInputFile(String filePath) {
        final Path resolvedPath;
        try {
            resolvedPath = Paths.get(filePath).toAbsolutePath().normalize();
        } catch (Exception ex) {
            throw new IngestionException("Invalid input file path: " + filePath, ex);
        }

        if (!Files.exists(resolvedPath)) {
            throw new IngestionException("Input file not found: " + resolvedPath);
        }
        if (!Files.isRegularFile(resolvedPath)) {
            throw new IngestionException("Input path is not a regular file: " + resolvedPath);
        }
        String extension = getFileExtension(resolvedPath);
        if (!extension.equals(".json") && !extension.equals(".csv")) {
            throw new IngestionException("Unsupported input format: " + extension + ". Only JSON and CSV are supported.");
        }

        return resolvedPath;
    }

    // Gets the input file size.
    private long getFileSize(Path inputFile) {
        try {
            return Files.size(inputFile);
        } catch (IOException ex) {
            LOG.error("Unable to determine input file size: {}", inputFile, ex);
            throw new IngestionException("Unable to determine input file size: " + inputFile, ex);
        }
    }

    // Launches the PySpark application using spark-submit.
    private List<String> invokeSparkSplitter(Path inputFile, Path outputDirectory) {
        List<String> splitFiles = new ArrayList<>();
        ProcessBuilder processBuilder = new ProcessBuilder(
                sparkSubmit,
                "--master", "local[*]", pysparkScript,
                "--input_file", inputFile.toString(),
                "--output_dir", outputDirectory.toString(),
                "--threshold_kb", String.valueOf(splitThresholdKb));
        processBuilder.redirectErrorStream(true);
        LOG.info("Starting Spark splitter: inputFile={}, outputDirectory={}, thresholdKb={}", inputFile, outputDirectory, splitThresholdKb);

        try {
            Process process = processBuilder.start();
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream(), StandardCharsets.UTF_8))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    if (line.startsWith(SPLIT_OUTPUT_PREFIX)) {
                        String outputFile = line.substring(SPLIT_OUTPUT_PREFIX.length()).trim();
                        if (!outputFile.isBlank()) {
                            splitFiles.add(outputFile);
                        }
                    } else {
                        LOG.debug("Spark splitter: {}", line);
                    }
                }
            }

            int exitCode = process.waitFor();
            if (exitCode != 0) {
                LOG.error("Spark splitter failed: exitCode={}, inputFile={}", exitCode, inputFile);
                throw new IngestionException("Spark splitter failed with exit code " + exitCode);
            }
            LOG.info("Spark splitter completed successfully: outputFiles={}", splitFiles.size());
            return splitFiles;

        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            LOG.error("Spark splitter process was interrupted: {}", inputFile, ex);
            throw new IngestionException("Spark splitter process was interrupted", ex);

        } catch (IOException ex) {
            LOG.error("Unable to start Spark splitter: {}", sparkSubmit, ex);
            throw new IngestionException("Unable to start Spark splitter", ex);
        }
    }

    private String getFileExtension(Path path) {

        String fileName = path.getFileName().toString();
        int index = fileName.lastIndexOf('.');
        if (index < 0) {
            return "";
        }
        return fileName.substring(index).toLowerCase();
    }
}