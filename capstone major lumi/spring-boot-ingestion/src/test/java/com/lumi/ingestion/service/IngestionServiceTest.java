package com.lumi.ingestion.service;

import com.lumi.ingestion.client.airflow.AirflowClient;
import com.lumi.ingestion.dto.request.TriggerRequest;
import com.lumi.ingestion.dto.response.TriggerResponse;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.AfterEach;
import org.mockito.ArgumentCaptor;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class IngestionServiceTest {

    private AirflowClient airflowClient;
    private IngestionService service;
    private Path testDataRoot;

    @BeforeEach
    void setUp() throws IOException {
        airflowClient = mock(AirflowClient.class);
        testDataRoot = Path.of("data", "test-service-" + UUID.randomUUID()).toAbsolutePath().normalize();
        Files.createDirectories(testDataRoot.resolve("control"));
        Files.createDirectories(testDataRoot.resolve("json"));
        Files.writeString(testDataRoot.resolve("json/employees.json"), "{}");
        Files.writeString(testDataRoot.resolve("control/test.properties"),
                "file_name=employees.json\nfile_path="
                        + testDataRoot.resolve("json/employees.json").toString().replace('\\', '/')
                        + "\nrecord_count=1\n");
        service = new IngestionService(
                airflowClient,
                "lumi_ingestion_dag",
                "jdbc:postgresql://postgres:5432/lumi",
                "lumi_user",
                "lumi_password",
                testDataRoot.resolve("errors").toString(),
                10_485_760L,
                testDataRoot.resolve("splits").toString(),
                "spark-submit",
                "/opt/lumi/pyspark/split_file.py"
        );
    }

    @AfterEach
    void tearDown() throws IOException {
        if (Files.exists(testDataRoot)) {
            try (var paths = Files.walk(testDataRoot)) {
                paths.sorted((left, right) -> right.compareTo(left)).forEach(path -> {
                    try { Files.deleteIfExists(path); } catch (IOException ignored) { }
                });
            }
        }
    }

    @Test
    void trigger_generatesExecutionId() {
        when(airflowClient.triggerDagRun(any(), any(), any())).thenReturn("lumi-run-1");

        TriggerRequest req = new TriggerRequest();
        req.setControlFile(testDataRoot.resolve("control/test.properties").toString());

        TriggerResponse response = service.trigger(req);

        assertNotNull(response.getExecutionId());
        assertFalse(response.getExecutionId().isBlank());
    }

    @Test
    void trigger_smallFile_doesNotCopyOrCreateSplitDirectory() throws IOException {
        when(airflowClient.triggerDagRun(any(), any(), any())).thenReturn("lumi-run-1");

        TriggerRequest req = new TriggerRequest();
        req.setControlFile(testDataRoot.resolve("control/test.properties").toString());

        service.trigger(req);

        Path splitBase = testDataRoot.resolve("splits");
        assertFalse(Files.exists(splitBase), "No split directory should be created for small files");
    }

    @Test
    void trigger_smallFile_passesOriginalDirectoryToAirflow() throws IOException {
        when(airflowClient.triggerDagRun(any(), any(), any())).thenReturn("lumi-run-1");

        TriggerRequest req = new TriggerRequest();
        req.setControlFile(testDataRoot.resolve("control/test.properties").toString());

        service.trigger(req);

        @SuppressWarnings("unchecked")
        ArgumentCaptor<Map<String, Object>> confCaptor = ArgumentCaptor.forClass(Map.class);
        verify(airflowClient).triggerDagRun(any(), any(), confCaptor.capture());

        @SuppressWarnings("unchecked")
        List<String> inputFiles = (List<String>) confCaptor.getValue().get("input_files");
        assertEquals(1, inputFiles.size());
        assertTrue(inputFiles.get(0).endsWith("/json/employees.json"),
                "Airflow should receive the original input file path, got: " + inputFiles.get(0));
    }

    @Test
    void trigger_smallFile_splitFlagIsFalse() {
        when(airflowClient.triggerDagRun(any(), any(), any())).thenReturn("lumi-run-1");

        TriggerRequest req = new TriggerRequest();
        req.setControlFile(testDataRoot.resolve("control/test.properties").toString());

        TriggerResponse response = service.trigger(req);

        assertFalse(response.isSplit(), "split flag must be false for small files");
        assertEquals(1, response.getInputFileCount());
    }

    @Test
    void trigger_passesExecutionIdToAirflow() {
        when(airflowClient.triggerDagRun(any(), any(), any())).thenReturn("lumi-run-1");
        TriggerRequest req = new TriggerRequest();
        req.setControlFile(testDataRoot.resolve("control/test.properties").toString());
        TriggerResponse response = service.trigger(req);
        @SuppressWarnings("unchecked")
        ArgumentCaptor<Map<String, Object>> confCaptor = ArgumentCaptor.forClass(Map.class);
        verify(airflowClient).triggerDagRun(eq("lumi_ingestion_dag"), anyString(), confCaptor.capture());

        assertEquals(response.getExecutionId(), confCaptor.getValue().get("execution_id"));
    }

    @Test
    void trigger_setsJsonFileFormat() {
        when(airflowClient.triggerDagRun(any(), any(), any())).thenReturn("lumi-run-1");
        TriggerRequest req = new TriggerRequest();
        req.setControlFile(testDataRoot.resolve("control/test.properties").toString());
        service.trigger(req);
        @SuppressWarnings("unchecked")
        ArgumentCaptor<Map<String, Object>> confCaptor = ArgumentCaptor.forClass(Map.class);
        verify(airflowClient).triggerDagRun(any(), any(), confCaptor.capture());
        assertEquals("json", confCaptor.getValue().get("file_format"));
    }

    @Test
    void trigger_confContainsAllRequiredBeamArgs() {
        when(airflowClient.triggerDagRun(any(), any(), any())).thenReturn("lumi-run-1");
        TriggerRequest req = new TriggerRequest();
        req.setControlFile(testDataRoot.resolve("control/test.properties").toString());
        service.trigger(req);
        @SuppressWarnings("unchecked")
        ArgumentCaptor<Map<String, Object>> confCaptor = ArgumentCaptor.forClass(Map.class);
        verify(airflowClient).triggerDagRun(any(), any(), confCaptor.capture());
        Map<String, Object> conf = confCaptor.getValue();
        assertTrue(conf.containsKey("input_files"));
        assertTrue(conf.containsKey("file_format"));
        assertEquals("1", conf.get("expected_record_count"));
        assertDoesNotThrow(() -> Instant.parse((String) conf.get("source_creation_time")));
        assertTrue(conf.containsKey("execution_id"));
        assertTrue(conf.containsKey("jdbc_url"));
        assertTrue(conf.containsKey("jdbc_username"));
        assertTrue(conf.containsKey("jdbc_password"));
        assertTrue(conf.containsKey("error_output"));
    }

    @Test
    void trigger_returnsTriggeredStatus() {
        when(airflowClient.triggerDagRun(any(), any(), any())).thenReturn("lumi-run-1");
        TriggerRequest req = new TriggerRequest();
        req.setControlFile(testDataRoot.resolve("control/test.properties").toString());
        TriggerResponse response = service.trigger(req);
        assertEquals("ACCEPTED", response.getStatus());
        assertEquals("lumi-run-1", response.getDagRunIds());
    }

    @Test
    void trigger_airflowFailure_propagatesException() {
        when(airflowClient.triggerDagRun(any(), any(), any()))
                .thenThrow(new org.springframework.web.client.RestClientException("connection refused"));
        TriggerRequest req = new TriggerRequest();
        req.setControlFile(testDataRoot.resolve("control/test.properties").toString());
        assertThrows(org.springframework.web.client.RestClientException.class,
                () -> service.trigger(req));
    }

    @Test
    void trigger_rejectsNegativeRecordCount() throws IOException {
        Path controlFile = testDataRoot.resolve("control/negative-count.properties");
        Files.writeString(controlFile, "file_name=employees.json\nfile_path="
                + testDataRoot.resolve("json/employees.json") + "\nrecord_count=-1\n");
        TriggerRequest req = new TriggerRequest();
        req.setControlFile(controlFile.toString());

        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class,
                () -> service.trigger(req));

        assertEquals("record_count cannot be negative", exception.getMessage());
        verifyNoInteractions(airflowClient);
    }

    @Test
    void trigger_rejectsUnsupportedInputFormat() throws IOException {
        Path unsupportedFile = testDataRoot.resolve("json/employees.txt");
        Files.writeString(unsupportedFile, "data");
        Path controlFile = testDataRoot.resolve("control/unsupported-format.properties");
        Files.writeString(controlFile, "file_name=employees.txt\nfile_path="
                + unsupportedFile.toString().replace('\\', '/') + "\nrecord_count=1\n");
        TriggerRequest req = new TriggerRequest();
        req.setControlFile(controlFile.toString());

        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class,
                () -> service.trigger(req));

        assertTrue(exception.getMessage().contains("Unsupported input format"));
        verifyNoInteractions(airflowClient);
    }

    @Test
    void trigger_rejectsMissingInputFile() throws IOException {
        Path controlFile = testDataRoot.resolve("control/missing-input.properties");
        Files.writeString(controlFile, "file_name=missing.json\nfile_path="
                + testDataRoot.resolve("json/missing.json").toString().replace('\\', '/') + "\nrecord_count=1\n");
        TriggerRequest req = new TriggerRequest();
        req.setControlFile(controlFile.toString());

        assertThrows(com.lumi.ingestion.exception.InputFileNotFoundException.class,
                () -> service.trigger(req));
        verifyNoInteractions(airflowClient);
    }

    @Test
    void trigger_rejectsFileNameThatDoesNotMatchFilePath() throws IOException {
        Path controlFile = testDataRoot.resolve("control/mismatched-name.properties");
        Files.writeString(controlFile, "file_name=other.json\nfile_path="
                + testDataRoot.resolve("json/employees.json").toString().replace('\\', '/')
                + "\nrecord_count=1\n");
        TriggerRequest req = new TriggerRequest();
        req.setControlFile(controlFile.toString());

        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class,
                () -> service.trigger(req));

        assertTrue(exception.getMessage().contains("other.json"));
        assertTrue(exception.getMessage().contains("employees.json"));
        verifyNoInteractions(airflowClient);
    }

    @Test
    void trigger_rejectsMissingFileName() throws IOException {
        Path controlFile = testDataRoot.resolve("control/missing-file-name.properties");
        Files.writeString(controlFile, "file_path="
                + testDataRoot.resolve("json/employees.json").toString().replace('\\', '/')
                + "\nrecord_count=1\n");
        TriggerRequest req = new TriggerRequest();
        req.setControlFile(controlFile.toString());

        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class,
                () -> service.trigger(req));

        assertTrue(exception.getMessage().contains("file_name"));
        verifyNoInteractions(airflowClient);
    }

    @Test
    void trigger_rejectsMissingFilePath() throws IOException {
        Path controlFile = testDataRoot.resolve("control/missing-file-path.properties");
        Files.writeString(controlFile, "file_name=employees.json\nrecord_count=1\n");
        TriggerRequest req = new TriggerRequest();
        req.setControlFile(controlFile.toString());

        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class,
                () -> service.trigger(req));

        assertTrue(exception.getMessage().contains("file_path"));
        verifyNoInteractions(airflowClient);
    }

    @Test
    void trigger_rejectsNonNumericRecordCount() throws IOException {
        Path controlFile = testDataRoot.resolve("control/invalid-count.properties");
        Files.writeString(controlFile, "file_name=employees.json\nfile_path="
                + testDataRoot.resolve("json/employees.json") + "\nrecord_count=invalid\n");
        TriggerRequest req = new TriggerRequest();
        req.setControlFile(controlFile.toString());

        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class,
                () -> service.trigger(req));

        assertEquals("record_count must be a valid number", exception.getMessage());
        verifyNoInteractions(airflowClient);
    }
}