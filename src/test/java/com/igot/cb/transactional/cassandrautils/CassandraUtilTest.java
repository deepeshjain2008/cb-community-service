package com.igot.cb.transactional.cassandrautils;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.cql.ColumnDefinition;
import com.datastax.oss.driver.api.core.cql.ColumnDefinitions;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class CassandraUtilTest {

    @Test
    void getPreparedStatementBuildsInsertQueryWithMultipleColumns() {
        Map<String, Object> request = new LinkedHashMap<>();
        request.put("id", "1");
        request.put("name", "test");

        String query = CassandraUtil.getPreparedStatement("test_keyspace", "test_table", request);

        assertEquals("INSERT INTO test_keyspace.test_table(id,name) VALUES (?,?);", query);
    }

    @Test
    void getPreparedStatementBuildsInsertQueryWithSingleColumn() {
        Map<String, Object> request = new LinkedHashMap<>();
        request.put("id", "1");

        String query = CassandraUtil.getPreparedStatement("test_keyspace", "test_table", request);

        assertEquals("INSERT INTO test_keyspace.test_table(id) VALUES (?);", query);
    }

    private ColumnDefinitions columnDefinitions(String... columnNames) {
        ColumnDefinitions columnDefinitions = mock(ColumnDefinitions.class);
        List<ColumnDefinition> defs = new java.util.ArrayList<>();
        for (String name : columnNames) {
            ColumnDefinition def = mock(ColumnDefinition.class);
            when(def.getName()).thenReturn(CqlIdentifier.fromInternal(name));
            defs.add(def);
        }
        // Mockito's default answer does not invoke real default interface methods,
        // so ColumnDefinitions#forEach (inherited from Iterable) must be stubbed directly
        // rather than relying on iterator() being called internally.
        doAnswer(invocation -> {
            Consumer<ColumnDefinition> consumer = invocation.getArgument(0);
            defs.forEach(consumer);
            return null;
        }).when(columnDefinitions).forEach(any());
        return columnDefinitions;
    }

    @Test
    void fetchColumnsMappingBuildsMapFromColumnDefinitions() {
        ResultSet resultSet = mock(ResultSet.class);
        ColumnDefinitions columnDefinitions = columnDefinitions("id", "name");
        when(resultSet.getColumnDefinitions()).thenReturn(columnDefinitions);

        Map<String, String> mapping = CassandraUtil.fetchColumnsMapping(resultSet);

        assertEquals("id", mapping.get("id"));
        assertEquals("name", mapping.get("name"));
    }

    @Test
    void createResponseBuildsListFromRows() {
        ResultSet resultSet = mock(ResultSet.class);
        ColumnDefinitions columnDefinitions = columnDefinitions("id");
        when(resultSet.getColumnDefinitions()).thenReturn(columnDefinitions);
        Row row = mock(Row.class);
        when(row.getObject("id")).thenReturn("value1");
        when(resultSet.iterator()).thenReturn(List.of(row).iterator());

        List<Map<String, Object>> result = CassandraUtil.createResponse(resultSet);

        assertEquals(1, result.size());
        assertEquals("value1", result.get(0).get("id"));
    }

    @Test
    void createResponseWithKeyBuildsMapKeyedByField() {
        ResultSet resultSet = mock(ResultSet.class);
        ColumnDefinitions columnDefinitions = columnDefinitions("id");
        when(resultSet.getColumnDefinitions()).thenReturn(columnDefinitions);
        Row row = mock(Row.class);
        when(row.getObject("id")).thenReturn("value1");
        when(resultSet.iterator()).thenReturn(List.of(row).iterator());

        Map<String, Object> result = CassandraUtil.createResponse(resultSet, "id");

        Map<String, Object> expectedRow = (Map<String, Object>) result.get("value1");
        assertEquals("value1", expectedRow.get("id"));
    }
}
