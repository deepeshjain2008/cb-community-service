package com.igot.cb.transactional.cassandrautils;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;

class CassandraPropertyReaderTest {

    @Test
    void getInstanceReturnsSameSingleton() {
        CassandraPropertyReader first = CassandraPropertyReader.getInstance();
        CassandraPropertyReader second = CassandraPropertyReader.getInstance();

        assertSame(first, second);
    }

    @Test
    void readPropertyReturnsKeyItselfWhenNotConfigured() {
        CassandraPropertyReader reader = CassandraPropertyReader.getInstance();

        String value = reader.readProperty("some_column_name");

        assertEquals("some_column_name", value);
    }
}
