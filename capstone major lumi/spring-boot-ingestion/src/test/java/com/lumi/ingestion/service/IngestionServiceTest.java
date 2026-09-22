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
   void trigger_passesExecutionIdToAirflow() {
       when(airflowClient.triggerDagRun(any(), any(), any())).thenReturn("lumi-run-1");
       TriggerRequest req = new TriggerRequest();
   req.setControlFile(testDataRoot.resolve("control/test.properties").toString());
       TriggerResponse response = service.trigger(req);
       @SuppressWarnings("unchecked")
       ArgumentCaptor<Map<String, String>> confCaptor = ArgumentCaptor.forClass(Map.class);
       verify(airflowClient).triggerDagRun(eq("lumi_ingestion_dag"), anyString(), confCaptor.capture());

       assertEquals(response.getExecutionId(), confCaptor.getValue().get("execution_id"));
   }

   @Test
   void trigger_passesInputDirectoryToAirflow() {
       when(airflowClient.triggerDagRun(any(), any(), any())).thenReturn("lumi-run-1");
       TriggerRequest req = new TriggerRequest();
   req.setControlFile(testDataRoot.resolve("control/test.properties").toString());
       service.trigger(req);
       @SuppressWarnings("unchecked")
       ArgumentCaptor<Map<String, String>> confCaptor = ArgumentCaptor.forClass(Map.class);
       verify(airflowClient).triggerDagRun(any(), any(), confCaptor.capture());
      assertTrue(confCaptor.getValue().get("input_directory").startsWith("/opt/lumi/data/splits/"));
   }

   @Test
   void trigger_setsJsonFileFormat() {
       when(airflowClient.triggerDagRun(any(), any(), any())).thenReturn("lumi-run-1");
       TriggerRequest req = new TriggerRequest();
   req.setControlFile(testDataRoot.resolve("control/test.properties").toString());
       service.trigger(req);
       @SuppressWarnings("unchecked")
       ArgumentCaptor<Map<String, String>> confCaptor = ArgumentCaptor.forClass(Map.class);
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
       ArgumentCaptor<Map<String, String>> confCaptor = ArgumentCaptor.forClass(Map.class);
       verify(airflowClient).triggerDagRun(any(), any(), confCaptor.capture());
       Map<String, String> conf = confCaptor.getValue();
    assertTrue(conf.containsKey("input_directory"));
    assertTrue(conf.containsKey("file_format"));
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
}
