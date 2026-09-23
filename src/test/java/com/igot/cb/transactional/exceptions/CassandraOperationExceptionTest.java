package com.igot.cb.transactional.exceptions;

import com.igot.cb.pores.exceptions.CustomException;
import com.igot.cb.pores.util.Constants;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpStatus;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;

class CassandraOperationExceptionTest {

    @Test
    void constructorSetsMessageCauseCodeAndStatus() {
        RuntimeException cause = new RuntimeException("session unavailable");

        CassandraOperationException exception = new CassandraOperationException(
            "Exception occurred while updating record", cause);

        assertEquals(Constants.ERROR, exception.getCode());
        assertEquals("Exception occurred while updating record", exception.getMessage());
        assertEquals(HttpStatus.INTERNAL_SERVER_ERROR, exception.getHttpStatusCode());
        assertSame(cause, exception.getCause());
        assertInstanceOf(CustomException.class, exception);
    }
}
