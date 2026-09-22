package com.lumi.ingestion.controller;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.lumi.ingestion.dto.request.TriggerRequest;
import com.lumi.ingestion.dto.response.TriggerResponse;
import com.lumi.ingestion.service.IngestionService;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

@WebMvcTest(IngestionController.class)
class IngestionControllerTest {

   @Autowired MockMvc mockMvc;
   @Autowired ObjectMapper objectMapper;
   @MockBean  IngestionService ingestionService;

   @Test
   void validRequest_returns202WithExecutionId() throws Exception {
       TriggerResponse stubResponse = new TriggerResponse(
               "test-exec-id", "lumi-test-exec-id", 1, false, "ACCEPTED");
       when(ingestionService.trigger(any())).thenReturn(stubResponse);

       TriggerRequest req = new TriggerRequest();
        req.setControlFile("data/control/employee_control.txt");

       mockMvc.perform(post("/api/ingestion/trigger")
                       .contentType(MediaType.APPLICATION_JSON)
                       .content(objectMapper.writeValueAsString(req)))
               .andExpect(status().isAccepted())
               .andExpect(jsonPath("$.executionId").value("test-exec-id"))
               .andExpect(jsonPath("$.dagRunIds").value("lumi-test-exec-id"))
               .andExpect(jsonPath("$.inputFileCount").value(1))
               .andExpect(jsonPath("$.status").value("ACCEPTED"));
   }

   @Test
   void blankInputFile_returns400() throws Exception {
       TriggerRequest req = new TriggerRequest();
        req.setControlFile("  ");

       mockMvc.perform(post("/api/ingestion/trigger")
                       .contentType(MediaType.APPLICATION_JSON)
                       .content(objectMapper.writeValueAsString(req)))
               .andExpect(status().isBadRequest())
               .andExpect(jsonPath("$.error").exists());
   }

   @Test
   void missingInputFile_returns400() throws Exception {
       mockMvc.perform(post("/api/ingestion/trigger")
                       .contentType(MediaType.APPLICATION_JSON)
                       .content("{}"))
               .andExpect(status().isBadRequest());
   }
}
