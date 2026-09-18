package com.igot.cb.pores.util;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.MapUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.*;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.RestTemplate;

import java.util.HashMap;
import java.util.Map;

@Service
@Slf4j
public class OutboundRequestHandlerServiceImpl {

  private Logger log = LoggerFactory.getLogger(OutboundRequestHandlerServiceImpl.class);

  private static final String ERROR_RESPONSE_LOG = "Error Response: {}";
  private static final String ERROR_PARSING_RESPONSE_LOG = "Error while parsing error response: {}";
  private static final String ERROR_LOGGING_RESPONSE_LOG = "Error while logging response: {}";
  private static final String DEBUG_LOG_FORMAT_SIX_ARGS = "{}{}{}{}{}{}";
  private static final String DEBUG_REQUEST_LOG_FORMAT = "{}{}{}{}{}{}{}{}{}";

  private final RestTemplate restTemplate;

  @Autowired
  public OutboundRequestHandlerServiceImpl(RestTemplate restTemplate) {
    this.restTemplate = restTemplate;
  }

  /**
   * @param uri
   * @param request
   * @return
   * @throws Exception
   */
  public Object fetchResultUsingPost(String uri, Object request) {
    ObjectMapper mapper = new ObjectMapper();
    mapper.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
    Object response = null;
    try {
      HttpHeaders headers = new HttpHeaders();
      headers.setContentType(MediaType.APPLICATION_JSON);
      HttpEntity<Object> entity = new HttpEntity<>(request, headers);
      if (log.isDebugEnabled()) {
        log.debug(DEBUG_REQUEST_LOG_FORMAT, this.getClass().getCanonicalName(), Constants.FETCH_RESULT_CONSTANT,
            System.lineSeparator(), Constants.URI_CONSTANT, uri, System.lineSeparator(),
            Constants.REQUEST_CONSTANT, mapper.writeValueAsString(request), System.lineSeparator());
      }
      response = restTemplate.postForObject(uri, entity, Map.class);
      if (log.isDebugEnabled()) {
        log.debug(DEBUG_LOG_FORMAT_SIX_ARGS, this.getClass().getCanonicalName(), Constants.FETCH_RESULT_CONSTANT,
            System.lineSeparator(), Constants.RESPONSE_CONSTANT, mapper.writeValueAsString(response),
            System.lineSeparator());
      }
    } catch (HttpClientErrorException e) {
      try {
        response = (new ObjectMapper()).readValue(e.getResponseBodyAsString(),
            new TypeReference<HashMap<String, Object>>() {
            });
      } catch (Exception e1) {
        log.error(ERROR_PARSING_RESPONSE_LOG, e1.getMessage());
      }
      log.error("Failed to get details. ", e);
    } catch (Exception e) {
      log.error(e.getMessage());
    }
    return response;
  }

  /**
   * @param uri
   * @return
   * @throws Exception
   */
  public Object fetchResult(String uri) {
    ObjectMapper mapper = new ObjectMapper();
    mapper.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
    Object response = null;
    try {
      if (log.isDebugEnabled()) {
        log.debug(DEBUG_LOG_FORMAT_SIX_ARGS, this.getClass().getCanonicalName(), Constants.FETCH_RESULT_CONSTANT,
            System.lineSeparator(), Constants.URI_CONSTANT, uri, System.lineSeparator());
      }
      response = restTemplate.getForObject(uri, Map.class);
    } catch (HttpClientErrorException e) {
      try {
        response = (new ObjectMapper()).readValue(e.getResponseBodyAsString(),
            new TypeReference<HashMap<String, Object>>() {
            });
      } catch (Exception e1) {
        log.error(ERROR_PARSING_RESPONSE_LOG, e1.getMessage());
      }
      log.error("Error received: {}", e.getResponseBodyAsString(), e);
    } catch (Exception e) {
      log.error(e.getMessage());
      try {
        log.warn(ERROR_RESPONSE_LOG, mapper.writeValueAsString(response));
      } catch (Exception e1) {
        log.error(ERROR_LOGGING_RESPONSE_LOG, e1.getMessage());
      }
    }
    return response;
  }

  /**
   * @param uri
   * @return
   * @throws Exception
   */
  public Object fetchUsingGetWithHeaders(String uri, Map<String, String> headersValues) {
    ObjectMapper mapper = new ObjectMapper();
    mapper.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
    ResponseEntity<Map> response = null;
    try {
      if (log.isDebugEnabled()) {
        log.debug(DEBUG_LOG_FORMAT_SIX_ARGS, this.getClass().getCanonicalName(), Constants.FETCH_RESULT_CONSTANT,
            System.lineSeparator(), Constants.URI_CONSTANT, uri, System.lineSeparator());
      }
      HttpHeaders headers = new HttpHeaders();
      if (!CollectionUtils.isEmpty(headersValues)) {
        headersValues.forEach(headers::set);
      }
      HttpEntity entity = new HttpEntity(headers);
      response = restTemplate.exchange(uri, HttpMethod.GET, entity, Map.class);
      return response.getBody();
    } catch (Exception e) {
      log.error(e.getMessage());
    }
    return null;
  }

  public Object fetchUsingGetWithHeadersProfile(String uri, Map<String, String> headersValues) {
    ObjectMapper mapper = new ObjectMapper();
    mapper.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
    Map<String, Object> response = null;
    try {
      if (log.isDebugEnabled()) {
        log.debug(DEBUG_LOG_FORMAT_SIX_ARGS, this.getClass().getCanonicalName(), Constants.FETCH_RESULT_CONSTANT,
            System.lineSeparator(), Constants.URI_CONSTANT, uri, System.lineSeparator());
      }
      HttpHeaders headers = new HttpHeaders();
      if (!CollectionUtils.isEmpty(headersValues)) {
        headersValues.forEach(headers::set);
      }
      HttpEntity<Object> entity = new HttpEntity<>(headers);
      response = restTemplate.exchange(uri, HttpMethod.GET, entity, Map.class).getBody();
    } catch (HttpClientErrorException e) {
      try {
        response = (new ObjectMapper()).readValue(e.getResponseBodyAsString(),
            new TypeReference<HashMap<String, Object>>() {
            });
      } catch (Exception e1) {
        log.error(ERROR_PARSING_RESPONSE_LOG, e1.getMessage());
      }
      log.error("Error received: {}", e.getResponseBodyAsString(), e);
    } catch (Exception e) {
      log.error(e.getMessage());
      try {
        log.warn(ERROR_RESPONSE_LOG, mapper.writeValueAsString(response));
      } catch (Exception e1) {
        log.error(ERROR_LOGGING_RESPONSE_LOG, e1.getMessage());
      }
    }
    return response;
  }

  public Map<String, Object> fetchResultUsingPost(String uri, Object request, Map<String, String> headersValues) {
    log.info("OutboundRequestHandlerService::fetchResultUsingPost:inside the method");
    ObjectMapper mapper = new ObjectMapper();
    mapper.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
    Map<String, Object> response = null;
    try {
      HttpHeaders headers = new HttpHeaders();
      if (!CollectionUtils.isEmpty(headersValues)) {
        headersValues.forEach(headers::set);
      }
      headers.setContentType(MediaType.APPLICATION_JSON);
      HttpEntity<Object> entity = new HttpEntity<>(request, headers);
      if (log.isDebugEnabled()) {
        log.debug(DEBUG_REQUEST_LOG_FORMAT, this.getClass().getCanonicalName(), ".fetchResult",
            System.lineSeparator(), "URI: ", uri, System.lineSeparator(),
            "Request: ", mapper.writeValueAsString(request), System.lineSeparator());
      }
      log.info("OutboundRequestHandlerService::fetchResultUsingPost: inside method: {}", uri);
      response = restTemplate.postForObject(uri, entity, Map.class);
      if (log.isDebugEnabled()) {
        log.debug("{}{}{}", "Response: ", mapper.writeValueAsString(response), System.lineSeparator());
      }
    } catch (HttpClientErrorException hce) {
      try {
        response = (new ObjectMapper()).readValue(hce.getResponseBodyAsString(),
            new TypeReference<HashMap<String, Object>>() {
            });
      } catch (Exception e1) {
        log.error(ERROR_PARSING_RESPONSE_LOG, e1.getMessage());
      }
      log.error("Error received: {}", hce.getResponseBodyAsString(), hce);
    } catch(JsonProcessingException e) {
      log.error(e.getMessage());
      try {
        log.warn(ERROR_RESPONSE_LOG, mapper.writeValueAsString(response));
      } catch (Exception e1) {
        log.error(ERROR_LOGGING_RESPONSE_LOG, e1.getMessage());
      }
    }
    return response;
  }

  public Map<String, Object> fetchResultUsingPatch(String uri, Object request, Map<String, String> headersValues) {
    Map<String, Object> response = null;
    try {
      HttpHeaders headers = new HttpHeaders();
      if (!CollectionUtils.isEmpty(headersValues)) {
        headersValues.forEach(headers::set);
      }
      headers.setContentType(MediaType.APPLICATION_JSON);
      HttpEntity<Object> entity = new HttpEntity<>(request, headers);
      logDetails(uri, request);
      response = restTemplate.patchForObject(uri, entity, Map.class);
      logDetails(uri, response);
    } catch (HttpClientErrorException e) {
      try {
        response = (new ObjectMapper()).readValue(e.getResponseBodyAsString(),
            new TypeReference<HashMap<String, Object>>() {
            });
      } catch (Exception e1) {
        log.error(ERROR_PARSING_RESPONSE_LOG, e1.getMessage());
      }
      log.error("Error received: {}", e.getResponseBodyAsString(), e);
    }
    if (response == null) {
      return MapUtils.EMPTY_MAP;
    }
    return response;
  }

  private void logDetails(String uri, Object objectDetails) {
    if (!log.isDebugEnabled()) {
      return;
    }
    try {
      log.debug(DEBUG_REQUEST_LOG_FORMAT, this.getClass().getCanonicalName(), ".fetchResult",
          System.lineSeparator(), "URI: ", uri, System.lineSeparator(),
          "Request/Response: ", (new ObjectMapper()).writeValueAsString(objectDetails), System.lineSeparator());
    } catch (JsonProcessingException je) {
      log.error("Error while logging details for URI: {}", uri, je);
    }
  }

  public Object fetchResultUsingPostAsString(String uri, Object request) {
    ObjectMapper mapper = new ObjectMapper();
    mapper.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
    Object response = null;
    try {
      HttpHeaders headers = new HttpHeaders();
      headers.setContentType(MediaType.APPLICATION_JSON);
      HttpEntity<Object> entity = new HttpEntity<>(request, headers);
      if (log.isDebugEnabled()) {
        log.debug(DEBUG_REQUEST_LOG_FORMAT, this.getClass().getCanonicalName(), Constants.FETCH_RESULT_CONSTANT,
            System.lineSeparator(), Constants.URI_CONSTANT, uri, System.lineSeparator(),
            Constants.REQUEST_CONSTANT, mapper.writeValueAsString(request), System.lineSeparator());
      }
      response = restTemplate.postForObject(uri, entity, String.class);
      if (log.isDebugEnabled()) {
        log.debug(DEBUG_LOG_FORMAT_SIX_ARGS, this.getClass().getCanonicalName(), Constants.FETCH_RESULT_CONSTANT,
            System.lineSeparator(), Constants.RESPONSE_CONSTANT, mapper.writeValueAsString(response),
            System.lineSeparator());
      }
    } catch (HttpClientErrorException e) {
      try {
        response = (new ObjectMapper()).readValue(e.getResponseBodyAsString(),
            new TypeReference<HashMap<String, Object>>() {
            });
      } catch (Exception e1) {
        log.error(ERROR_PARSING_RESPONSE_LOG, e1.getMessage());
      }
      log.error("Failed to get details. ", e);
    } catch (Exception e) {
      log.error(e.getMessage());
    }
    return response;
  }

}
