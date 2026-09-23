package com.igot.cb.transactional.cassandrautils;

import com.datastax.oss.driver.api.core.CqlSession;
import com.igot.cb.transactional.exceptions.CassandraOperationException;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.ColumnDefinitions;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.igot.cb.pores.util.ApiResponse;
import com.igot.cb.pores.util.Constants;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class CassandraOperationImplTest {

    @Mock
    private CassandraConnectionManager connectionManager;

    @Mock
    private CqlSession session;

    private CassandraOperationImpl cassandraOperation;

    @BeforeEach
    void setUp() {
        cassandraOperation = new CassandraOperationImpl(connectionManager);
        lenient().when(connectionManager.getSession(anyString())).thenReturn(session);
    }

    private ResultSet emptyResultSet() {
        ResultSet resultSet = mock(ResultSet.class);
        ColumnDefinitions columnDefinitions = mock(ColumnDefinitions.class);
        when(resultSet.getColumnDefinitions()).thenReturn(columnDefinitions);
        when(resultSet.iterator()).thenReturn(Collections.emptyIterator());
        return resultSet;
    }

    @Test
    void insertRecordReturnsSuccessResponse() {
        PreparedStatement preparedStatement = mock(PreparedStatement.class);
        BoundStatement boundStatement = mock(BoundStatement.class);
        when(session.prepare(anyString())).thenReturn(preparedStatement);
        when(preparedStatement.bind(any(Object[].class))).thenReturn(boundStatement);

        Map<String, Object> request = new HashMap<>();
        request.put("id", "1");
        request.put("name", "test");

        Object result = cassandraOperation.insertRecord("test_keyspace", "test_table", request);

        assertTrue(result instanceof ApiResponse);
        ApiResponse response = (ApiResponse) result;
        assertEquals(Constants.SUCCESS, response.get(Constants.RESPONSE));
    }

    @Test
    void insertRecordReturnsFailureResponseOnException() {
        when(session.prepare(anyString())).thenThrow(new RuntimeException("connection lost"));

        Map<String, Object> request = new HashMap<>();
        request.put("id", "1");

        Object result = cassandraOperation.insertRecord("test_keyspace", "test_table", request);

        ApiResponse response = (ApiResponse) result;
        assertEquals(Constants.FAILED, response.get(Constants.RESPONSE));
        assertTrue(((String) response.get(Constants.ERROR_MESSAGE)).contains("connection lost"));
    }

    @Test
    void getRecordsByPropertiesWithoutFilteringReturnsEmptyListOnEmptyResultSet() {
        ResultSet resultSet = emptyResultSet();
        when(session.execute(any(com.datastax.oss.driver.api.core.cql.SimpleStatement.class)))
            .thenReturn(resultSet);

        Map<String, Object> propertyMap = new HashMap<>();
        propertyMap.put("id", Arrays.asList("1", "2"));
        propertyMap.put("status", "active");

        List<Map<String, Object>> result = cassandraOperation.getRecordsByPropertiesWithoutFiltering(
            "test_keyspace", "test_table", propertyMap, Arrays.asList("id", "status"), 10);

        assertTrue(result.isEmpty());
    }

    @Test
    void getRecordsByPropertiesWithoutFilteringHandlesEmptyPropertyMapNoFieldsNoLimit() {
        ResultSet resultSet = emptyResultSet();
        when(session.execute(any(com.datastax.oss.driver.api.core.cql.SimpleStatement.class)))
            .thenReturn(resultSet);

        List<Map<String, Object>> result = cassandraOperation.getRecordsByPropertiesWithoutFiltering(
            "test_keyspace", "test_table", new HashMap<>(), null, null);

        assertTrue(result.isEmpty());
    }

    @Test
    void getRecordsByPropertiesWithoutFilteringHandlesEmptyListPropertyValue() {
        ResultSet resultSet = emptyResultSet();
        when(session.execute(any(com.datastax.oss.driver.api.core.cql.SimpleStatement.class)))
            .thenReturn(resultSet);

        Map<String, Object> propertyMap = new HashMap<>();
        propertyMap.put("id", new ArrayList<String>());

        List<Map<String, Object>> result = cassandraOperation.getRecordsByPropertiesWithoutFiltering(
            "test_keyspace", "test_table", propertyMap, null, null);

        assertTrue(result.isEmpty());
    }

    @Test
    void getRecordsByPropertiesWithoutFilteringReturnsEmptyListOnException() {
        when(connectionManager.getSession(anyString())).thenThrow(new RuntimeException("no connection"));

        List<Map<String, Object>> result = cassandraOperation.getRecordsByPropertiesWithoutFiltering(
            "test_keyspace", "test_table", new HashMap<>(), null, null);

        assertTrue(result.isEmpty());
    }

    @Test
    void updateRecordReturnsSuccessResponse() {
        when(session.execute(any(com.datastax.oss.driver.api.core.cql.SimpleStatement.class)))
            .thenReturn(mock(ResultSet.class));

        Map<String, Object> updateAttributes = new HashMap<>();
        updateAttributes.put("status", "inactive");
        Map<String, Object> compositeKey = new HashMap<>();
        compositeKey.put("id", "1");

        Map<String, Object> result = cassandraOperation.updateRecord("test_keyspace", "test_table", updateAttributes, compositeKey);

        assertEquals(Constants.SUCCESS, result.get(Constants.RESPONSE));
    }

    @Test
    void updateRecordThrowsAndReturnsFailureOnException() {
        when(connectionManager.getSession(anyString())).thenThrow(new RuntimeException("update failed"));

        Map<String, Object> updateAttributes = new HashMap<>();
        updateAttributes.put("status", "inactive");
        Map<String, Object> compositeKey = new HashMap<>();
        compositeKey.put("id", "1");

        assertThrows(CassandraOperationException.class, () ->
            cassandraOperation.updateRecord("test_keyspace", "test_table", updateAttributes, compositeKey));
    }
}
