package com.igot.cb.pores.util;

import com.fasterxml.jackson.databind.JsonNode;
import com.igot.cb.pores.exceptions.CustomException;
import com.networknt.schema.JsonSchema;
import com.networknt.schema.ValidationMessage;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;

import java.util.Set;

@Slf4j
@Service
public class PayloadValidation {

  private final JsonSchemaCache schemaCache;

  private Logger logger = LoggerFactory.getLogger(PayloadValidation.class);

  @Autowired
  public PayloadValidation(JsonSchemaCache schemaCache) {
    this.schemaCache = schemaCache;
  }

  public void validatePayload(String schemaKey, JsonNode payload) {
    try {
      JsonSchema schema = schemaCache.getSchema(schemaKey);

      if (schema == null) {
        String errorMsg = String.format("Schema not found for key: %s", schemaKey);
        throw new CustomException(errorMsg, errorMsg, HttpStatus.BAD_REQUEST);
      }

      if (payload.isArray()) {
        for (JsonNode objectNode : payload) {
          validateObject(schema, objectNode);
        }
      } else {
        validateObject(schema, payload);
      }
    } catch (Exception e) {
      logger.error("Failed to validate payload", e);
      throw new CustomException("Failed to validate payload", e.getMessage(), HttpStatus.BAD_REQUEST);
    }
  }

  private void validateObject(JsonSchema schema, JsonNode objectNode) {
    Set<ValidationMessage> validationMessages = schema.validate(objectNode);
    if (!validationMessages.isEmpty()) {
      StringBuilder errorMessage = new StringBuilder("Validation error(s): \n");
      for (ValidationMessage message : validationMessages) {
        errorMessage.append(message.getMessage()).append("\n");
      }
      logger.error("Validation Error: {}", errorMessage);
      throw new CustomException("Validation Error", errorMessage.toString(), HttpStatus.BAD_REQUEST);
    }
  }
}

