package com.lumi.ingestion.client.airflow;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Map;

/* Client for triggering Airflow DAG runs through the Airflow REST API.*/
@Component
public class AirflowClient {

    private static final Logger LOG = LoggerFactory.getLogger(AirflowClient.class);
    private final RestTemplate restTemplate;
    private final String airflowBaseUrl;
    private final String airflowUsername;
    private final String airflowPassword;

    public AirflowClient(RestTemplate restTemplate, @Value("${airflow.base-url}") String airflowBaseUrl, @Value("${airflow.username}") String airflowUsername, @Value("${airflow.password}") String airflowPassword) {
        this.restTemplate = restTemplate;
        this.airflowBaseUrl = airflowBaseUrl;
        this.airflowUsername = airflowUsername;
        this.airflowPassword = airflowPassword;
    }

    public String triggerDagRun(String dagId, String dagRunId, Map<String, String> conf) {
        String url = airflowBaseUrl + "/api/v1/dags/" + dagId + "/dagRuns";
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        headers.set(HttpHeaders.AUTHORIZATION, basicAuth(airflowUsername, airflowPassword));
        Map<String, Object> body = Map.of("dag_run_id", dagRunId, "conf", conf);
        HttpEntity<Map<String, Object>> request = new HttpEntity<>(body, headers);
        LOG.info("Triggering Airflow DAG: dagId={}, dagRunId={}", dagId, dagRunId);

        try {
            @SuppressWarnings("unchecked")
            Map<String, Object> response = restTemplate.postForObject(url, request, Map.class);
            String confirmedRunId = response != null ? (String) response.get("dag_run_id") : dagRunId;
            LOG.info("Airflow DAG triggered successfully: dagId={}, dagRunId={}", dagId, confirmedRunId);
            return confirmedRunId;

        } catch (Exception ex) {
            LOG.error("Failed to trigger Airflow DAG: dagId={}, dagRunId={}, url={}", dagId, dagRunId, url, ex);
            throw ex;
        }
    }

    private String basicAuth(String username, String password) {
        String credentials = username + ":" + password;
        return "Basic " + Base64.getEncoder().encodeToString(credentials.getBytes(StandardCharsets.UTF_8));
    }
}