package com.igot.cb.pores.util;

import org.junit.jupiter.api.Test;
import org.springframework.http.HttpStatus;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ApiResponseTest {

    @Test
    void defaultConstructorInitializesVerTsAndParams() {
        ApiResponse response = new ApiResponse();

        assertEquals("v1", response.getVer());
        assertNotNull(response.getTs());
        assertNotNull(response.getParams());
        assertNotNull(response.getParams().getResMsgId());
    }

    @Test
    void idConstructorSetsId() {
        ApiResponse response = new ApiResponse("resp-1");

        assertEquals("resp-1", response.getId());
        assertEquals("v1", response.getVer());
    }

    @Test
    void settersAndGettersRoundTrip() {
        ApiResponse response = new ApiResponse();
        ApiRespParam params = new ApiRespParam("param-id");

        response.setId("id1");
        response.setVer("v2");
        response.setTs("2024-01-01");
        response.setParams(params);
        response.setResponseCode(HttpStatus.OK);

        assertEquals("id1", response.getId());
        assertEquals("v2", response.getVer());
        assertEquals("2024-01-01", response.getTs());
        assertEquals(params, response.getParams());
        assertEquals(HttpStatus.OK, response.getResponseCode());
    }

    @Test
    void resultMapOperations() {
        ApiResponse response = new ApiResponse();

        response.put("key1", "value1");
        assertEquals("value1", response.get("key1"));
        assertTrue(response.containsKey("key1"));
        assertFalse(response.containsKey("missing"));

        Map<String, Object> extra = new HashMap<>();
        extra.put("key2", "value2");
        response.putAll(extra);
        assertEquals("value2", response.get("key2"));

        Map<String, Object> replacement = new HashMap<>();
        replacement.put("key3", "value3");
        response.setResult(replacement);
        assertEquals(replacement, response.getResult());
        assertFalse(response.containsKey("key1"));
    }
}
