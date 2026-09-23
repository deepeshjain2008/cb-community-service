package com.igot.cb.transactional.service;

import ch.qos.logback.classic.Level;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.slf4j.LoggerFactory;
import org.springframework.http.*;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.RestTemplate;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class RequestHandlerServiceImplTest {

    @Mock
    private RestTemplate restTemplate;

    private RequestHandlerServiceImpl service;

    private static Level originalLevel;

    @BeforeAll
    static void enableDebugLogging() {
        ch.qos.logback.classic.Logger logger =
            (ch.qos.logback.classic.Logger) LoggerFactory.getLogger(RequestHandlerServiceImpl.class);
        originalLevel = logger.getLevel();
        logger.setLevel(Level.DEBUG);
    }

    @AfterAll
    static void restoreLogging() {
        ch.qos.logback.classic.Logger logger =
            (ch.qos.logback.classic.Logger) LoggerFactory.getLogger(RequestHandlerServiceImpl.class);
        logger.setLevel(originalLevel);
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    static class BadBean {
        public String getValue() {
            throw new RuntimeException("cannot serialize");
        }
    }

    private RequestHandlerServiceImpl newService() {
        return new RequestHandlerServiceImpl(restTemplate);
    }

    @Test
    void fetchResultUsingPostReturnsResponseOnSuccess() {
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
    void fetchResultUsingPostParsesHttpClientErrorBody() {
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
    void fetchResultUsingPostHandlesUnparsableHttpClientErrorBody() {
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
    void fetchResultUsingPostHandlesJsonProcessingExceptionOnRequestSerialization() {
        service = newService();

        Map<String, Object> result = service.fetchResultUsingPost("http://test.com", new BadBean(), null);

        assertNull(result);
    }

    @Test
    void fetchResultUsingPostHandlesJsonProcessingExceptionOnResponseSerialization() {
        service = newService();
        Map<String, Object> badResponse = new HashMap<>();
        badResponse.put("bad", new BadBean());
        when(restTemplate.postForObject(any(String.class), any(HttpEntity.class), eq(Map.class)))
            .thenReturn(badResponse);

        Map<String, Object> result = service.fetchResultUsingPost("http://test.com", "request", null);

        assertEquals(badResponse, result);
    }

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
}
