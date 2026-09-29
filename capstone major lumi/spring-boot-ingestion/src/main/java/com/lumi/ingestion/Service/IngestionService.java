package com.lumi.ingestion.service;

import com.lumi.ingestion.client.airflow.AirflowClient;
import com.lumi.ingestion.dto.request.TriggerRequest;
import com.lumi.ingestion.dto.response.TriggerResponse;
import com.lumi.ingestion.exception.InputFileNotFoundException;
import com.lumi.ingestion.exception.SparkSplitterException;
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
import java.nio.file.attribute.BasicFileAttributes;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
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
    private static final String AIRFLOW_DATA_ROOT = "/opt/lumi/data";
    private static final int SPARK_OUTPUT_TAIL_LINES = 40;
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
    private final String pythonExecutable;

    public IngestionService(
            AirflowClient airflowClient,
            @Value("${airflow.dag-id}") String dagId,
            @Value("${spring.datasource.url}") String jdbcUrl,
            @Value("${spring.datasource.username}") String jdbcUsername,
            @Value("${spring.datasource.password}") String jdbcPassword,
            @Value("${file.error-output-base}") String errorOutputBase,
            @Value("${file.split-threshold-kb}") long splitThresholdKb,
            @Value("${file.split-output-base}") String splitOutputBase,
            @Value("${file.spark-submit}") String sparkSubmit,
            @Value("${file.pyspark-script}") String pysparkScript,
            @Value("${file.python-executable}") String pythonExecutable) {
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
        this.pythonExecutable = pythonExecutable;
    }

    public TriggerResponse trigger(TriggerRequest request) {
        String executionId = UUID.randomUUID().toString();
        LOG.info("Starting ingestion: executionId={}, controlFile={}", executionId, request.getControlFile());

        Properties controlProperties = readControlFile(request.getControlFile());
        String fileName = getRequiredProperty(controlProperties, "file_name");
        String filePath = getRequiredProperty(controlProperties, "file_path");
        String recordCount = getRequiredProperty(controlProperties, "record_count");
        validateRecordCount(recordCount);

        LOG.info("Control file loaded: executionId={}, fileName={}, expectedRecordCount={}", executionId, fileName, recordCount);
        Path inputFile = resolveInputFile(filePath);
        if (!inputFile.getFileName().toString().equals(fileName)) {
            throw new IllegalArgumentException("file_name '" + fileName
                    + "' does not match the filename in file_path '"
                    + inputFile.getFileName() + "'");
        }
        Instant sourceCreationTime = getSourceCreationTime(inputFile);
        long fileSizeBytes = getFileSize(inputFile);
        long thresholdBytes = splitThresholdKb * 1024;
        boolean splitRequired = fileSizeBytes > thresholdBytes;
        LOG.info("File split decision: executionId={}, file={}, sizeBytes={}, thresholdBytes={}, splitRequired={}",
            executionId, inputFile, fileSizeBytes, thresholdBytes, splitRequired);
        List<String> inputFiles;
        if (splitRequired) {
            LOG.info("File exceeds split threshold. Starting Spark splitter: executionId={}", executionId);
            Path splitDirectory = Paths.get(splitOutputBase, executionId);
            inputFiles = invokeSparkSplitter(inputFile, splitDirectory);
            if (inputFiles.isEmpty()) {
            throw new SparkSplitterException("Spark exited successfully but produced no split files for " + inputFile);
            }
            LOG.info("Spark splitting completed: executionId={}, splitFiles={}", executionId, inputFiles.size());
        } else {
            inputFiles = List.of(inputFile.toString());
            LOG.info("File is within split threshold. No splitting required: executionId={}, inputFile={}", executionId, inputFile);
        }

        String dagRunId = triggerDagRun(executionId, inputFiles, recordCount, sourceCreationTime);
        LOG.info("Ingestion request accepted: executionId={}, inputFiles={}, dagRunId={}", executionId, inputFiles.size(), dagRunId);
        return new TriggerResponse(executionId, dagRunId, inputFiles.size(), splitRequired, "ACCEPTED");
    }

    private String triggerDagRun(String executionId, List<String> inputFiles, String recordCount, Instant sourceCreationTime) {
        String fileFormat = getFileExtension(Paths.get(inputFiles.get(0))).replace(".", "");
        String dagRunId = DAG_RUN_PREFIX + executionId;
        Path localErrorOutput = Paths.get(errorOutputBase, executionId, "errors");
        String airflowErrorOutput = toAirflowDataPath(localErrorOutput);

        List<String> airflowInputFiles = inputFiles.stream()
                .map(f -> toAirflowDataPath(Paths.get(f)))
                .toList();

        Map<String, Object> conf = new HashMap<>();
        conf.put("execution_id", executionId);
        conf.put("input_files", airflowInputFiles);
        conf.put("file_format", fileFormat);
        conf.put("expected_record_count", recordCount);
        conf.put("source_creation_time", sourceCreationTime.toString());
        conf.put("jdbc_url", jdbcUrl);
        conf.put("jdbc_username", jdbcUsername);
        conf.put("jdbc_password", jdbcPassword);
        conf.put("error_output", airflowErrorOutput);
        LOG.info("Triggering Airflow DAG: executionId={}, dagRunId={}, fileCount={}", executionId, dagRunId, airflowInputFiles.size());
        LOG.debug("Airflow input files: {}", airflowInputFiles);
        return airflowClient.triggerDagRun(dagId, dagRunId, conf);
    }

    private String toAirflowDataPath(Path localPath) {
        Path absolutePath = localPath.toAbsolutePath().normalize();
        Path projectDataDirectory = Paths.get("data").toAbsolutePath().normalize();
        if (!absolutePath.startsWith(projectDataDirectory)) {
            throw new IllegalArgumentException("File must be located under the project data directory. File: " + absolutePath + ", data directory: " + projectDataDirectory);
        }
        Path relativePath = projectDataDirectory.relativize(absolutePath);
        return Paths.get(AIRFLOW_DATA_ROOT, relativePath.toString()).toString().replace('\\', '/');
    }

    private Properties readControlFile(String controlFilePath) {
        Path path;
        try {
            path = Paths.get(controlFilePath).toAbsolutePath().normalize();
        } catch (Exception ex) {
            throw new IllegalArgumentException("Invalid control file path: " + controlFilePath, ex);
        }

        if (!Files.exists(path)) {
            throw new InputFileNotFoundException("Control file not found: " + path);
        }

        if (!Files.isRegularFile(path)) {
            throw new IllegalArgumentException("Control file is not a regular file: " + path);
        }

        Properties properties = new Properties();
        try (InputStream inputStream = Files.newInputStream(path)) {
            properties.load(inputStream);
            return properties;
        } catch (IOException ex) {
            LOG.error("Failed to read control file: {}", path, ex);
            throw new IllegalStateException("Failed to read control file: " + path, ex);
        }
    }

    private String getRequiredProperty(Properties properties, String propertyName) {
        String value = properties.getProperty(propertyName);
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(propertyName + " is missing in control file");
        }
        return value.trim();
    }

    private void validateRecordCount(String recordCount) {
        try {
            long count = Long.parseLong(recordCount);
            if (count < 0) {
                throw new IllegalArgumentException("record_count cannot be negative");
            }
        } catch (NumberFormatException ex) {
            throw new IllegalArgumentException("record_count must be a valid number", ex);
        }
    }

    private Path resolveInputFile(String filePath) {
        final Path resolvedPath;
        try {
            resolvedPath = Paths.get(filePath).toAbsolutePath().normalize();
        } catch (Exception ex) {
            throw new IllegalArgumentException("Invalid input file path: " + filePath, ex);
        }

        if (!Files.exists(resolvedPath)) {
            throw new InputFileNotFoundException("Input file not found: " + resolvedPath);
        }
        if (!Files.isRegularFile(resolvedPath)) {
            throw new IllegalArgumentException("Input path is not a regular file: " + resolvedPath);
        }
        String extension = getFileExtension(resolvedPath);
        if (!extension.equals(".json") && !extension.equals(".csv")) {
            throw new IllegalArgumentException("Unsupported input format: " + extension + ". Only JSON and CSV are supported.");
        }

        return resolvedPath;
    }

    private long getFileSize(Path inputFile) {
        try {
            return Files.size(inputFile);
        } catch (IOException ex) {
            LOG.error("Unable to determine input file size: {}", inputFile, ex);
            throw new IllegalStateException("Unable to determine input file size: " + inputFile, ex);
        }
    }

    private Instant getSourceCreationTime(Path inputFile) {
        try {
            return Files.readAttributes(inputFile, BasicFileAttributes.class).creationTime().toInstant();
        } catch (IOException ex) {
            LOG.error("Unable to determine source file creation time: {}", inputFile, ex);
            throw new IllegalStateException("Unable to determine source file creation time: " + inputFile, ex);
        }
    }

    private List<String> invokeSparkSplitter(Path inputFile, Path outputDirectory) {
        List<String> splitFiles = new ArrayList<>();
        Deque<String> outputTail = new ArrayDeque<>();
        ProcessBuilder processBuilder = new ProcessBuilder(
                "cmd.exe", "/c", sparkSubmit,
                "--master", "local[*]", pysparkScript,
                "--input_file", inputFile.toString(),
                "--output_dir", outputDirectory.toString(),
                "--threshold_kb", String.valueOf(splitThresholdKb)
        );
        processBuilder.environment().put("PYSPARK_PYTHON", pythonExecutable);
        processBuilder.environment().put("PYSPARK_DRIVER_PYTHON", pythonExecutable);

        processBuilder.redirectErrorStream(true);
        LOG.info("Launching Spark splitter: command={}, pythonExecutable={}, inputFile={}, outputDirectory={}",
                processBuilder.command(), pythonExecutable, inputFile, outputDirectory);
        try {
            Process process = processBuilder.start();
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream(), StandardCharsets.UTF_8))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    LOG.info("Spark splitter output: {}", line);
                    if (outputTail.size() == SPARK_OUTPUT_TAIL_LINES) {
                        outputTail.removeFirst();
                    }
                    outputTail.addLast(line);
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
                throw new SparkSplitterException("Spark splitter failed with exit code " + exitCode
                        + " for " + inputFile + ". Recent Spark output:\n" + String.join("\n", outputTail));
            }

            for (String splitFile : splitFiles) {
                if (!Files.isRegularFile(Paths.get(splitFile))) {
                    throw new SparkSplitterException("Spark reported a split file that does not exist: " + splitFile
                            + ". Recent Spark output:\n" + String.join("\n", outputTail));
                }
            }

            LOG.info("Spark splitter completed: exitCode={}, outputFiles={}", exitCode, splitFiles.size());
            return splitFiles;

        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            LOG.error("Spark splitter process was interrupted: {}", inputFile, ex);
            throw new SparkSplitterException("Spark splitter process was interrupted for " + inputFile, ex);
        } catch (IOException ex) {
            LOG.error("Unable to start Spark splitter: command={}", processBuilder.command(), ex);
            throw new SparkSplitterException("Unable to start Spark splitter. Command: "
                    + processBuilder.command() + ". Cause: " + ex.getMessage(), ex);
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