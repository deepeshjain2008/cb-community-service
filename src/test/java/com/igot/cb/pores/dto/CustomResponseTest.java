package com.igot.cb.pores.dto;

import org.junit.jupiter.api.Test;
import org.springframework.http.HttpStatus;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;

class CustomResponseTest {

    @Test
    void getParamsLazilyInitializesWhenNull() {
        CustomResponse response = new CustomResponse();

        RespParam params = response.getParams();

        assertNotNull(params);
        assertSame(params, response.getParams());
    }

    @Test
    void getParamsReturnsExistingInstanceWhenAlreadySet() {
        CustomResponse response = new CustomResponse();
        RespParam explicit = new RespParam("res", "msg", null, "SUCCESS", null);
        response.setParams(explicit);

        assertSame(explicit, response.getParams());
    }

    @Test
    void allArgsConstructorAndSettersRoundTrip() {
        Map<String, Object> result = new HashMap<>();
        result.put("k", "v");
        RespParam params = new RespParam("res", "msg", "err", "SUCCESS", "errmsg");

        CustomResponse response = new CustomResponse("hello", params, HttpStatus.OK, result);

        assertEquals("hello", response.getMessage());
        assertEquals(params, response.getParams());
        assertEquals(HttpStatus.OK, response.getResponseCode());
        assertEquals(result, response.getResult());

        response.setMessage("updated");
        response.setResponseCode(HttpStatus.BAD_REQUEST);
        assertEquals("updated", response.getMessage());
        assertEquals(HttpStatus.BAD_REQUEST, response.getResponseCode());
    }
}
