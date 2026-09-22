package com.lumi.ingestion.client;

import com.lumi.ingestion.client.airflow.AirflowClient;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.client.RestClientTest;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.web.client.MockRestServiceServer;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.*;
import static org.springframework.test.web.client.response.MockRestResponseCreators.withSuccess;

@RestClientTest(AirflowClient.class)
@TestPropertySource(properties = {
       "airflow.base-url=http://airflow:8080",
       "airflow.username=admin",
       "airflow.password=admin"
})
class AirflowClientTest {

   @Autowired AirflowClient airflowClient;
   @Autowired MockRestServiceServer server;

   @Test
   void triggerDagRun_sendsCorrectRequestAndReturnsRunId() {
       server.expect(requestTo("http://airflow:8080/api/v1/dags/lumi_ingestion_dag/dagRuns"))
             .andExpect(method(HttpMethod.POST))
             .andExpect(header("Authorization", "Basic YWRtaW46YWRtaW4="))
             .andRespond(withSuccess(
                     "{\"dag_run_id\":\"lumi-abc-123\"}",
                     MediaType.APPLICATION_JSON));

       String runId = airflowClient.triggerDagRun(
               "lumi_ingestion_dag",
               "lumi-abc-123",
               Map.of("input_file", "data/json/employees.json", "execution_id", "abc-123"));

       assertEquals("lumi-abc-123", runId);
       server.verify();
   }
}
