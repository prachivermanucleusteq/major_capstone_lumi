package com.lumi.ingestion.client;

import com.lumi.ingestion.client.airflow.AirflowClient;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.web.client.RestClientException;
import org.springframework.web.client.RestTemplate;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class AirflowClientTest {

	private static final String URL = "http://airflow:8080/api/v1/dags/lumi_ingestion_dag/dagRuns";

	@Test
	void triggerDagRun_returnsRunIdFromAirflowAndSendsAuth() {
		RestTemplate restTemplate = mock(RestTemplate.class);
		AirflowClient client = new AirflowClient(restTemplate, "http://airflow:8080", "admin", "admin");
		when(restTemplate.postForObject(eq(URL), any(HttpEntity.class), eq(Map.class)))
				.thenReturn(Map.of("dag_run_id", "confirmed-run"));

		String runId = client.triggerDagRun("lumi_ingestion_dag", "requested-run", Map.of("execution_id", "abc"));

		assertEquals("confirmed-run", runId);
		verify(restTemplate).postForObject(eq(URL), argThat((HttpEntity<?> request) -> {
		    Map<?, ?> body = (Map<?, ?>) request.getBody();
		    return "Basic YWRtaW46YWRtaW4=".equals(
			    request.getHeaders().getFirst(HttpHeaders.AUTHORIZATION))
			    && "requested-run".equals(body.get("dag_run_id"))
			    && Map.of("execution_id", "abc").equals(body.get("conf"));
		}), eq(Map.class));
	}

	@Test
	void triggerDagRun_usesRequestedRunIdWhenAirflowReturnsNoBody() {
		RestTemplate restTemplate = mock(RestTemplate.class);
		AirflowClient client = new AirflowClient(restTemplate, "http://airflow:8080", "admin", "secret");
		when(restTemplate.postForObject(eq(URL), any(HttpEntity.class), eq(Map.class))).thenReturn(null);

		assertEquals("requested-run", client.triggerDagRun("lumi_ingestion_dag", "requested-run", Map.of()));
	}

	@Test
	void triggerDagRun_propagatesAirflowFailure() {
		RestTemplate restTemplate = mock(RestTemplate.class);
		AirflowClient client = new AirflowClient(restTemplate, "http://airflow:8080", "admin", "secret");
		when(restTemplate.postForObject(eq(URL), any(HttpEntity.class), eq(Map.class)))
				.thenThrow(new RestClientException("Airflow unavailable"));

		assertThrows(RestClientException.class,
				() -> client.triggerDagRun("lumi_ingestion_dag", "requested-run", Map.of()));
	}
}
