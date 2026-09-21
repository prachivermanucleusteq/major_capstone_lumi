package com.lumi.ingestion.service;

import com.lumi.ingestion.client.airflow.AirflowClient;
import com.lumi.ingestion.dto.request.TriggerRequest;
import com.lumi.ingestion.dto.response.TriggerResponse;
import com.lumi.ingestion.exception.InputFileNotFoundException;
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
import java.nio.file.StandardCopyOption;
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
    private static final String AIRFLOW_DATA_ROOT = "/opt/lumi/data";
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
            @Value("${spring.datasource.url}") String jdbcUrl,
            @Value("${spring.datasource.username}") String jdbcUsername,
            @Value("${spring.datasource.password}") String jdbcPassword,
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
        long fileSizeBytes = getFileSize(inputFile);
        long thresholdBytes = splitThresholdKb * 1024;
        LOG.info("Input file size checked: executionId={}, file={}, sizeBytes={}, thresholdKb={}", executionId, inputFile, fileSizeBytes, splitThresholdKb);
        Path executionDirectory = Paths.get(splitOutputBase, executionId);
        List<String> inputFiles;
        if (fileSizeBytes > thresholdBytes) {
            LOG.info("File exceeds split threshold. Starting Spark splitter: executionId={}", executionId);
            inputFiles = invokeSparkSplitter(inputFile, executionDirectory);
            if (inputFiles.isEmpty()) {
                throw new IllegalStateException("Spark splitter produced no output files");
            }
            LOG.info("Spark splitting completed: executionId={}, splitFiles={}", executionId, inputFiles.size());
        } else {
            try {
                Files.createDirectories(executionDirectory);
                Path executionInputFile = executionDirectory.resolve(inputFile.getFileName());
                Files.copy(inputFile, executionInputFile, StandardCopyOption.REPLACE_EXISTING);
                inputFiles = List.of(executionInputFile.toString());

            } catch (IOException ex) {
                LOG.error("Failed to prepare execution input directory: executionId={}, directory={}", executionId, executionDirectory, ex);
                throw new IllegalStateException("Failed to prepare execution input directory", ex);
            }

            LOG.info("File is within split threshold. No splitting required: executionId={}, inputFile={}", executionId, inputFiles.get(0));
        }

        String dagRunId = triggerDagRun(executionId, executionDirectory, inputFiles);
        LOG.info("Ingestion request accepted: executionId={}, inputFiles={}, dagRunId={}", executionId, inputFiles.size(), dagRunId);
        return new TriggerResponse(executionId, dagRunId, inputFiles.size(), fileSizeBytes > thresholdBytes, "ACCEPTED");
    }

    private String triggerDagRun(String executionId, Path executionDirectory, List<String> inputFiles) {
        String airflowInputDirectory = toAirflowDataPath(executionDirectory);
        String fileFormat = getFileExtension(Paths.get(inputFiles.get(0))).replace(".", "");
        String dagRunId = DAG_RUN_PREFIX + executionId;
        Path localErrorOutput = Paths.get(errorOutputBase, executionId, "errors");

        String airflowErrorOutput = toAirflowDataPath(localErrorOutput);
        Map<String, String> conf = new HashMap<>();
        conf.put("execution_id", executionId);
        conf.put("input_directory", airflowInputDirectory);
        conf.put("file_format", fileFormat);
        conf.put("jdbc_url", jdbcUrl);
        conf.put("jdbc_username", jdbcUsername);
        conf.put("jdbc_password", jdbcPassword);
        conf.put("error_output", airflowErrorOutput);
        LOG.info("Triggering Airflow DAG once: executionId={}, dagRunId={}, inputDirectory={}, fileCount={}", executionId, dagRunId, airflowInputDirectory, inputFiles.size());
        LOG.debug("Airflow paths: inputDirectory={}, errorOutput={}", airflowInputDirectory, airflowErrorOutput);
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

    private List<String> invokeSparkSplitter(Path inputFile, Path outputDirectory) {
        List<String> splitFiles = new ArrayList<>();
        String sparkHome = "C:\\Spark\\spark-3.5.6-bin-hadoop3-scala2.13";
        String sparkSubmit = sparkHome + "\\bin\\spark-submit.cmd";
        String pythonHome = "C:\\Users\\user\\AppData\\Local\\Programs\\Python\\Python310";
        String pythonExecutable = pythonHome + "\\python.exe";
        ProcessBuilder processBuilder = new ProcessBuilder(
                "cmd.exe", "/c", sparkSubmit,
                "--master", "local[*]", pysparkScript,
                "--input_file", inputFile.toString(),
                "--output_dir", outputDirectory.toString(),
                "--threshold_kb", String.valueOf(splitThresholdKb)
        );
        Map<String, String> environment = processBuilder.environment();
        environment.put("SPARK_HOME", sparkHome);
        environment.put("PYSPARK_PYTHON", pythonExecutable);
        environment.put("PYSPARK_DRIVER_PYTHON", pythonExecutable);
        String currentPath = environment.get("PATH");
        environment.put("PATH", sparkHome + "\\bin;" + pythonHome + ";" + pythonHome + "\\Scripts;" + currentPath);

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

            LOG.info("Spark submit command: {}", processBuilder.command());
            int exitCode = process.waitFor();
            if (exitCode != 0) {
                LOG.error("Spark splitter failed: exitCode={}, inputFile={}", exitCode, inputFile);
                throw new IllegalStateException("Spark splitter failed with exit code " + exitCode);
            }

            LOG.info("Spark splitter completed successfully: outputFiles={}", splitFiles.size());
            return splitFiles;

        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            LOG.error("Spark splitter process was interrupted: {}", inputFile, ex);
            throw new IllegalStateException("Spark splitter process was interrupted", ex);
        } catch (IOException ex) {
            LOG.error("Unable to start Spark splitter: {}", sparkSubmit, ex);
            throw new IllegalStateException("Unable to start Spark splitter", ex);
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