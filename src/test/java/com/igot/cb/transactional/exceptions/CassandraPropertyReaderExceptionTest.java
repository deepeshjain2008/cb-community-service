package com.igot.cb.transactional.exceptions;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;

class CassandraPropertyReaderExceptionTest {

    @Test
    void constructorSetsMessageAndCause() {
        Throwable cause = new RuntimeException("root cause");

        CassandraPropertyReaderException exception = new CassandraPropertyReaderException("error message", cause);

        assertEquals("error message", exception.getMessage());
        assertSame(cause, exception.getCause());
    }
}
