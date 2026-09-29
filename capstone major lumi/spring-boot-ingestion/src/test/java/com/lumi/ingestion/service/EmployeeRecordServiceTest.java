package com.lumi.ingestion.service;

import com.lumi.ingestion.dto.response.EmployeeRecordResponse;
import com.lumi.ingestion.exception.EmployeeRecordException;
import com.lumi.ingestion.repository.EmployeeRecordRepository;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class EmployeeRecordServiceTest {

    @Test
    void findRecords_returnsRepositoryRecords() {
        EmployeeRecordRepository repository = mock(EmployeeRecordRepository.class);
        EmployeeRecordService service = new EmployeeRecordService(repository);
        List<EmployeeRecordResponse> expected = List.of();
        when(repository.findAll()).thenReturn(expected);

        List<EmployeeRecordResponse> actual = service.findRecords();

        assertSame(expected, actual);
        verify(repository).findAll();
    }

    @Test
    void findRecords_propagatesRepositoryFailure() {
        EmployeeRecordRepository repository = mock(EmployeeRecordRepository.class);
        EmployeeRecordService service = new EmployeeRecordService(repository);
        EmployeeRecordException failure = new EmployeeRecordException("database unavailable", new RuntimeException());
        when(repository.findAll()).thenThrow(failure);

        EmployeeRecordException actual = assertThrows(EmployeeRecordException.class, service::findRecords);

        assertSame(failure, actual);
        assertEquals("database unavailable", actual.getMessage());
    }
}