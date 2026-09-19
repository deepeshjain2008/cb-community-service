package com.igot.cb.pores.util;

import ch.qos.logback.classic.Level;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.RestTemplate;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class OutboundRequestHandlerServiceImplTest {

    @Mock
    private RestTemplate restTemplate;

    private OutboundRequestHandlerServiceImpl service;

    private static Level originalLevel;

    @BeforeAll
    static void enableDebugLogging() {
        ch.qos.logback.classic.Logger logger =
            (ch.qos.logback.classic.Logger) LoggerFactory.getLogger(OutboundRequestHandlerServiceImpl.class);
        originalLevel = logger.getLevel();
        logger.setLevel(Level.DEBUG);
    }

    @AfterAll
    static void restoreLogging() {
        ch.qos.logback.classic.Logger logger =
            (ch.qos.logback.classic.Logger) LoggerFactory.getLogger(OutboundRequestHandlerServiceImpl.class);
        logger.setLevel(originalLevel);
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    static class BadBean {
        public String getValue() {
            throw new RuntimeException("cannot serialize");
        }
    }

    private OutboundRequestHandlerServiceImpl newService() {
        return new OutboundRequestHandlerServiceImpl(restTemplate);
    }

    // ---- fetchResultUsingPost(uri, request) ----

    @Test
    void fetchResultUsingPostTwoArgReturnsResponseOnSuccess() {
        service = newService();
        Map<String, Object> expected = new HashMap<>();
        expected.put("status", "ok");
        when(restTemplate.postForObject(eq("http://test.com"), any(HttpEntity.class), eq(Map.class)))
            .thenReturn(expected);

        Object result = service.fetchResultUsingPost("http://test.com", "request-body");

        assertEquals(expected, result);
    }

    @Test
    void fetchResultUsingPostTwoArgParsesHttpClientErrorBody() {
        service = newService();
        HttpClientErrorException exception = HttpClientErrorException.create(
            HttpStatus.BAD_REQUEST, "Bad Request", HttpHeaders.EMPTY,
            "{\"error\":\"bad request\"}".getBytes(StandardCharsets.UTF_8), StandardCharsets.UTF_8);
        when(restTemplate.postForObject(any(String.class), any(HttpEntity.class), eq(Map.class)))
            .thenThrow(exception);

        Object result = service.fetchResultUsingPost("http://test.com", "request");

        assertEquals("bad request", ((Map<?, ?>) result).get("error"));
    }

    @Test
    void fetchResultUsingPostTwoArgHandlesUnparsableHttpClientErrorBody() {
        service = newService();
        HttpClientErrorException exception = HttpClientErrorException.create(
            HttpStatus.BAD_REQUEST, "Bad Request", HttpHeaders.EMPTY,
            "not-json".getBytes(StandardCharsets.UTF_8), StandardCharsets.UTF_8);
        when(restTemplate.postForObject(any(String.class), any(HttpEntity.class), eq(Map.class)))
            .thenThrow(exception);

        Object result = service.fetchResultUsingPost("http://test.com", "request");

        assertNull(result);
    }

    @Test
    void fetchResultUsingPostTwoArgHandlesGenericException() {
        service = newService();
        when(restTemplate.postForObject(any(String.class), any(HttpEntity.class), eq(Map.class)))
            .thenThrow(new IllegalStateException("boom"));

        Object result = service.fetchResultUsingPost("http://test.com", "request");

        assertNull(result);
    }

    @Test
    void fetchResultUsingPostTwoArgHandlesBadRequestSerialization() {
        service = newService();

        Object result = service.fetchResultUsingPost("http://test.com", new BadBean());

        assertNull(result);
    }

    // ---- fetchResult(uri) ----

    @Test
    void fetchResultReturnsResponseOnSuccess() {
        service = newService();
        Map<String, Object> expected = new HashMap<>();
        expected.put("status", "ok");
        when(restTemplate.getForObject(eq("http://test.com"), eq(Map.class))).thenReturn(expected);

        Object result = service.fetchResult("http://test.com");

        assertEquals(expected, result);
    }

    @Test
    void fetchResultParsesHttpClientErrorBody() {
        service = newService();
        HttpClientErrorException exception = HttpClientErrorException.create(
            HttpStatus.BAD_REQUEST, "Bad Request", HttpHeaders.EMPTY,
            "{\"error\":\"bad request\"}".getBytes(StandardCharsets.UTF_8), StandardCharsets.UTF_8);
        when(restTemplate.getForObject(any(String.class), eq(Map.class))).thenThrow(exception);

        Object result = service.fetchResult("http://test.com");

        assertEquals("bad request", ((Map<?, ?>) result).get("error"));
    }

    @Test
    void fetchResultHandlesUnparsableHttpClientErrorBody() {
        service = newService();
        HttpClientErrorException exception = HttpClientErrorException.create(
            HttpStatus.BAD_REQUEST, "Bad Request", HttpHeaders.EMPTY,
            "not-json".getBytes(StandardCharsets.UTF_8), StandardCharsets.UTF_8);
        when(restTemplate.getForObject(any(String.class), eq(Map.class))).thenThrow(exception);

        Object result = service.fetchResult("http://test.com");

        assertNull(result);
    }

    @Test
    void fetchResultHandlesGenericException() {
        service = newService();
        when(restTemplate.getForObject(any(String.class), eq(Map.class)))
            .thenThrow(new IllegalStateException("boom"));

        Object result = service.fetchResult("http://test.com");

        assertNull(result);
    }

    // ---- fetchUsingGetWithHeaders(uri, headersValues) ----

    @Test
    void fetchUsingGetWithHeadersReturnsBodyWithHeaders() {
        service = newService();
        Map<String, String> headers = new HashMap<>();
        headers.put("X-Test", "value");
        Map<String, Object> expected = new HashMap<>();
        expected.put("status", "ok");
        when(restTemplate.exchange(eq("http://test.com"), eq(HttpMethod.GET), any(HttpEntity.class), eq(Map.class)))
            .thenReturn(ResponseEntity.ok(expected));

        Object result = service.fetchUsingGetWithHeaders("http://test.com", headers);

        assertEquals(expected, result);
    }

    @Test
    void fetchUsingGetWithHeadersReturnsBodyWithoutHeaders() {
        service = newService();
        Map<String, Object> expected = new HashMap<>();
        expected.put("status", "ok");
        when(restTemplate.exchange(eq("http://test.com"), eq(HttpMethod.GET), any(HttpEntity.class), eq(Map.class)))
            .thenReturn(ResponseEntity.ok(expected));

        Object result = service.fetchUsingGetWithHeaders("http://test.com", null);

        assertEquals(expected, result);
    }

    @Test
    void fetchUsingGetWithHeadersHandlesException() {
        service = newService();
        when(restTemplate.exchange(any(String.class), eq(HttpMethod.GET), any(HttpEntity.class), eq(Map.class)))
            .thenThrow(new IllegalStateException("boom"));

        Object result = service.fetchUsingGetWithHeaders("http://test.com", null);

        assertNull(result);
    }

    // ---- fetchUsingGetWithHeadersProfile(uri, headersValues) ----

    @Test
    void fetchUsingGetWithHeadersProfileReturnsBodyOnSuccess() {
        service = newService();
        Map<String, String> headers = new HashMap<>();
        headers.put("X-Test", "value");
        Map<String, Object> expected = new HashMap<>();
        expected.put("status", "ok");
        when(restTemplate.exchange(eq("http://test.com"), eq(HttpMethod.GET), any(HttpEntity.class), eq(Map.class)))
            .thenReturn(ResponseEntity.ok(expected));

        Object result = service.fetchUsingGetWithHeadersProfile("http://test.com", headers);

        assertEquals(expected, result);
    }

    @Test
    void fetchUsingGetWithHeadersProfileParsesHttpClientErrorBody() {
        service = newService();
        HttpClientErrorException exception = HttpClientErrorException.create(
            HttpStatus.NOT_FOUND, "Not Found", HttpHeaders.EMPTY,
            "{\"error\":\"missing\"}".getBytes(StandardCharsets.UTF_8), StandardCharsets.UTF_8);
        when(restTemplate.exchange(any(String.class), eq(HttpMethod.GET), any(HttpEntity.class), eq(Map.class)))
            .thenThrow(exception);

        Object result = service.fetchUsingGetWithHeadersProfile("http://test.com", null);

        assertEquals("missing", ((Map<?, ?>) result).get("error"));
    }

    @Test
    void fetchUsingGetWithHeadersProfileHandlesUnparsableHttpClientErrorBody() {
        service = newService();
        HttpClientErrorException exception = HttpClientErrorException.create(
            HttpStatus.NOT_FOUND, "Not Found", HttpHeaders.EMPTY,
            "not-json".getBytes(StandardCharsets.UTF_8), StandardCharsets.UTF_8);
        when(restTemplate.exchange(any(String.class), eq(HttpMethod.GET), any(HttpEntity.class), eq(Map.class)))
            .thenThrow(exception);

        Object result = service.fetchUsingGetWithHeadersProfile("http://test.com", null);

        assertNull(result);
    }

    @Test
    void fetchUsingGetWithHeadersProfileHandlesGenericException() {
        service = newService();
        when(restTemplate.exchange(any(String.class), eq(HttpMethod.GET), any(HttpEntity.class), eq(Map.class)))
            .thenThrow(new IllegalStateException("boom"));

        Object result = service.fetchUsingGetWithHeadersProfile("http://test.com", null);

        assertNull(result);
    }

    // ---- fetchResultUsingPost(uri, request, headersValues) ----

    @Test
    void fetchResultUsingPostThreeArgReturnsResponseWithHeaders() {
        service = newService();
        Map<String, String> headers = new HashMap<>();
        headers.put("X-Test", "value");
        Map<String, Object> expected = new HashMap<>();
        expected.put("status", "ok");
        when(restTemplate.postForObject(eq("http://test.com"), any(HttpEntity.class), eq(Map.class)))
            .thenReturn(expected);

        Map<String, Object> result = service.fetchResultUsingPost("http://test.com", "request-body", headers);

        assertEquals(expected, result);
    }

    @Test
    void fetchResultUsingPostThreeArgReturnsResponseWithoutHeaders() {
        service = newService();
        Map<String, Object> expected = new HashMap<>();
        expected.put("status", "ok");
        when(restTemplate.postForObject(eq("http://test.com"), any(HttpEntity.class), eq(Map.class)))
            .thenReturn(expected);

        Map<String, Object> result = service.fetchResultUsingPost("http://test.com", "request-body", null);

        assertEquals(expected, result);
    }

    @Test
    void fetchResultUsingPostThreeArgParsesHttpClientErrorBody() {
        service = newService();
        HttpClientErrorException exception = HttpClientErrorException.create(
            HttpStatus.BAD_REQUEST, "Bad Request", HttpHeaders.EMPTY,
            "{\"error\":\"bad request\"}".getBytes(StandardCharsets.UTF_8), StandardCharsets.UTF_8);
        when(restTemplate.postForObject(any(String.class), any(HttpEntity.class), eq(Map.class)))
            .thenThrow(exception);

        Map<String, Object> result = service.fetchResultUsingPost("http://test.com", "request", null);

        assertEquals("bad request", result.get("error"));
    }

    @Test
    void fetchResultUsingPostThreeArgHandlesUnparsableHttpClientErrorBody() {
        service = newService();
        HttpClientErrorException exception = HttpClientErrorException.create(
            HttpStatus.BAD_REQUEST, "Bad Request", HttpHeaders.EMPTY,
            "not-json".getBytes(StandardCharsets.UTF_8), StandardCharsets.UTF_8);
        when(restTemplate.postForObject(any(String.class), any(HttpEntity.class), eq(Map.class)))
            .thenThrow(exception);

        Map<String, Object> result = service.fetchResultUsingPost("http://test.com", "request", null);

        assertNull(result);
    }

    @Test
    void fetchResultUsingPostThreeArgHandlesJsonProcessingExceptionOnRequest() {
        service = newService();

        Map<String, Object> result = service.fetchResultUsingPost("http://test.com", new BadBean(), null);

        assertNull(result);
    }

    @Test
    void fetchResultUsingPostThreeArgHandlesJsonProcessingExceptionOnResponse() {
        service = newService();
        Map<String, Object> badResponse = new HashMap<>();
        badResponse.put("bad", new BadBean());
        when(restTemplate.postForObject(any(String.class), any(HttpEntity.class), eq(Map.class)))
            .thenReturn(badResponse);

        Map<String, Object> result = service.fetchResultUsingPost("http://test.com", "request", null);

        assertEquals(badResponse, result);
    }

    @Test
    void fetchResultUsingPostThreeArgPropagatesUncaughtRuntimeException() {
        service = newService();
        when(restTemplate.postForObject(any(String.class), any(HttpEntity.class), eq(Map.class)))
            .thenThrow(new IllegalStateException("boom"));

        assertThrows(IllegalStateException.class, () ->
            service.fetchResultUsingPost("http://test.com", "request", null));
    }

    // ---- fetchResultUsingPatch(uri, request, headersValues) ----

    @Test
    void fetchResultUsingPatchReturnsResponseWithHeaders() {
        service = newService();
        Map<String, String> headers = new HashMap<>();
        headers.put("X-Test", "value");
        Map<String, Object> expected = new HashMap<>();
        expected.put("status", "ok");
        when(restTemplate.patchForObject(eq("http://test.com"), any(HttpEntity.class), eq(Map.class)))
            .thenReturn(expected);

        Map<String, Object> result = service.fetchResultUsingPatch("http://test.com", "request-body", headers);

        assertEquals(expected, result);
    }

    @Test
    void fetchResultUsingPatchReturnsEmptyMapWhenResponseNull() {
        service = newService();
        when(restTemplate.patchForObject(eq("http://test.com"), any(HttpEntity.class), eq(Map.class)))
            .thenReturn(null);

        Map<String, Object> result = service.fetchResultUsingPatch("http://test.com", "request-body", null);

        assertTrue(result.isEmpty());
    }

    @Test
    void fetchResultUsingPatchParsesHttpClientErrorBody() {
        service = newService();
        HttpClientErrorException exception = HttpClientErrorException.create(
            HttpStatus.BAD_REQUEST, "Bad Request", HttpHeaders.EMPTY,
            "{\"error\":\"bad request\"}".getBytes(StandardCharsets.UTF_8), StandardCharsets.UTF_8);
        when(restTemplate.patchForObject(any(String.class), any(HttpEntity.class), eq(Map.class)))
            .thenThrow(exception);

        Map<String, Object> result = service.fetchResultUsingPatch("http://test.com", "request", null);

        assertEquals("bad request", result.get("error"));
    }

    @Test
    void fetchResultUsingPatchHandlesUnparsableHttpClientErrorBody() {
        service = newService();
        HttpClientErrorException exception = HttpClientErrorException.create(
            HttpStatus.BAD_REQUEST, "Bad Request", HttpHeaders.EMPTY,
            "not-json".getBytes(StandardCharsets.UTF_8), StandardCharsets.UTF_8);
        when(restTemplate.patchForObject(any(String.class), any(HttpEntity.class), eq(Map.class)))
            .thenThrow(exception);

        Map<String, Object> result = service.fetchResultUsingPatch("http://test.com", "request", null);

        assertTrue(result.isEmpty());
    }

    @Test
    void fetchResultUsingPatchHandlesBadRequestSerializationInLogDetails() {
        service = newService();
        Map<String, Object> expected = new HashMap<>();
        expected.put("status", "ok");
        when(restTemplate.patchForObject(any(String.class), any(HttpEntity.class), eq(Map.class)))
            .thenReturn(expected);

        Map<String, Object> result = service.fetchResultUsingPatch("http://test.com", new BadBean(), null);

        assertEquals(expected, result);
    }

    @Test
    void fetchResultUsingPatchSkipsLogDetailsWhenDebugDisabled() {
        ch.qos.logback.classic.Logger logger =
            (ch.qos.logback.classic.Logger) LoggerFactory.getLogger(OutboundRequestHandlerServiceImpl.class);
        logger.setLevel(Level.INFO);
        try {
            service = newService();
            Map<String, Object> expected = new HashMap<>();
            expected.put("status", "ok");
            when(restTemplate.patchForObject(any(String.class), any(HttpEntity.class), eq(Map.class)))
                .thenReturn(expected);

            Map<String, Object> result = service.fetchResultUsingPatch("http://test.com", "request-body", null);

            assertEquals(expected, result);
        } finally {
            logger.setLevel(Level.DEBUG);
        }
    }

    // ---- fetchResultUsingPostAsString(uri, request) ----

    @Test
    void fetchResultUsingPostAsStringReturnsResponseOnSuccess() {
        service = newService();
        when(restTemplate.postForObject(eq("http://test.com"), any(HttpEntity.class), eq(String.class)))
            .thenReturn("response-body");

        Object result = service.fetchResultUsingPostAsString("http://test.com", "request-body");

        assertEquals("response-body", result);
    }

    @Test
    void fetchResultUsingPostAsStringParsesHttpClientErrorBody() {
        service = newService();
        HttpClientErrorException exception = HttpClientErrorException.create(
            HttpStatus.BAD_REQUEST, "Bad Request", HttpHeaders.EMPTY,
            "{\"error\":\"bad request\"}".getBytes(StandardCharsets.UTF_8), StandardCharsets.UTF_8);
        when(restTemplate.postForObject(any(String.class), any(HttpEntity.class), eq(String.class)))
            .thenThrow(exception);

        Object result = service.fetchResultUsingPostAsString("http://test.com", "request");

        assertEquals("bad request", ((Map<?, ?>) result).get("error"));
    }

    @Test
    void fetchResultUsingPostAsStringHandlesUnparsableHttpClientErrorBody() {
        service = newService();
        HttpClientErrorException exception = HttpClientErrorException.create(
            HttpStatus.BAD_REQUEST, "Bad Request", HttpHeaders.EMPTY,
            "not-json".getBytes(StandardCharsets.UTF_8), StandardCharsets.UTF_8);
        when(restTemplate.postForObject(any(String.class), any(HttpEntity.class), eq(String.class)))
            .thenThrow(exception);

        Object result = service.fetchResultUsingPostAsString("http://test.com", "request");

        assertNull(result);
    }

    @Test
    void fetchResultUsingPostAsStringHandlesGenericException() {
        service = newService();
        when(restTemplate.postForObject(any(String.class), any(HttpEntity.class), eq(String.class)))
            .thenThrow(new IllegalStateException("boom"));

        Object result = service.fetchResultUsingPostAsString("http://test.com", "request");

        assertNull(result);
    }

    @Test
    void fetchResultUsingPostAsStringHandlesBadRequestSerialization() {
        service = newService();

        Object result = service.fetchResultUsingPostAsString("http://test.com", new BadBean());

        assertNull(result);
    }

    // ---- debug-disabled variants (covers the false branch of every log.isDebugEnabled() guard) ----

    @Test
    void allMethodsSkipDebugLoggingWhenDebugDisabled() {
        ch.qos.logback.classic.Logger logger =
            (ch.qos.logback.classic.Logger) LoggerFactory.getLogger(OutboundRequestHandlerServiceImpl.class);
        logger.setLevel(Level.INFO);
        try {
            service = newService();
            Map<String, Object> expected = new HashMap<>();
            expected.put("status", "ok");
            when(restTemplate.postForObject(any(String.class), any(HttpEntity.class), eq(Map.class)))
                .thenReturn(expected);
            when(restTemplate.getForObject(any(String.class), eq(Map.class))).thenReturn(expected);
            when(restTemplate.exchange(any(String.class), eq(HttpMethod.GET), any(HttpEntity.class), eq(Map.class)))
                .thenReturn(ResponseEntity.ok(expected));
            when(restTemplate.postForObject(any(String.class), any(HttpEntity.class), eq(String.class)))
                .thenReturn("response-body");

            assertEquals(expected, service.fetchResultUsingPost("http://test.com", "request-body"));
            assertEquals(expected, service.fetchResult("http://test.com"));
            assertEquals(expected, service.fetchUsingGetWithHeaders("http://test.com", null));
            assertEquals(expected, service.fetchUsingGetWithHeadersProfile("http://test.com", null));
            assertEquals(expected, service.fetchResultUsingPost("http://test.com", "request-body", null));
            assertEquals("response-body", service.fetchResultUsingPostAsString("http://test.com", "request-body"));
        } finally {
            logger.setLevel(Level.DEBUG);
        }
    }
}
