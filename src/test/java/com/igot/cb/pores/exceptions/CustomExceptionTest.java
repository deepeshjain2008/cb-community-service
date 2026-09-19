package com.igot.cb.pores.exceptions;

import org.junit.jupiter.api.Test;
import org.springframework.http.HttpStatus;

import static org.junit.jupiter.api.Assertions.assertEquals;

class CustomExceptionTest {

    @Test
    void constructorSetsAllFields() {
        CustomException exception = new CustomException("ERR001", "Something went wrong", HttpStatus.BAD_REQUEST);

        assertEquals("ERR001", exception.getCode());
        assertEquals("Something went wrong", exception.getMessage());
        assertEquals(HttpStatus.BAD_REQUEST, exception.getHttpStatusCode());
    }
}
