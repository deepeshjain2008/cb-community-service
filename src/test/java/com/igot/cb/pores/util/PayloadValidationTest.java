package com.igot.cb.pores.util;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.igot.cb.pores.exceptions.CustomException;
import com.networknt.schema.JsonSchema;
import com.networknt.schema.JsonSchemaFactory;
import com.networknt.schema.SpecVersion;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class PayloadValidationTest {

    private static final String SCHEMA_JSON = "{"
        + "\"$schema\":\"http://json-schema.org/draft-07/schema#\","
        + "\"type\":\"object\","
        + "\"properties\":{\"name\":{\"type\":\"string\"}},"
        + "\"required\":[\"name\"]"
        + "}";

    @Mock
    private JsonSchemaCache schemaCache;

    private PayloadValidation payloadValidation;
    private final ObjectMapper objectMapper = new ObjectMapper();

    @BeforeEach
    void setUp() {
        payloadValidation = new PayloadValidation(schemaCache);
    }

    private JsonSchema realSchema() {
        return JsonSchemaFactory.getInstance(SpecVersion.VersionFlag.V7).getSchema(SCHEMA_JSON);
    }

    @Test
    void validatePayloadPassesForValidObject() throws Exception {
        when(schemaCache.getSchema("mySchema")).thenReturn(realSchema());
        JsonNode payload = objectMapper.readTree("{\"name\":\"test\"}");

        assertDoesNotThrow(() -> payloadValidation.validatePayload("mySchema", payload));
    }

    @Test
    void validatePayloadThrowsForInvalidObject() throws Exception {
        when(schemaCache.getSchema("mySchema")).thenReturn(realSchema());
        JsonNode payload = objectMapper.readTree("{\"other\":\"value\"}");

        assertThrows(CustomException.class, () -> payloadValidation.validatePayload("mySchema", payload));
    }

    @Test
    void validatePayloadValidatesEachElementOfArray() throws Exception {
        when(schemaCache.getSchema("mySchema")).thenReturn(realSchema());
        JsonNode payload = objectMapper.readTree("[{\"name\":\"a\"},{\"name\":\"b\"}]");

        assertDoesNotThrow(() -> payloadValidation.validatePayload("mySchema", payload));
    }

    @Test
    void validatePayloadThrowsWhenArrayElementInvalid() throws Exception {
        when(schemaCache.getSchema("mySchema")).thenReturn(realSchema());
        JsonNode payload = objectMapper.readTree("[{\"name\":\"a\"},{\"bad\":\"b\"}]");

        assertThrows(CustomException.class, () -> payloadValidation.validatePayload("mySchema", payload));
    }

    @Test
    void validatePayloadThrowsWhenSchemaNotFound() throws Exception {
        when(schemaCache.getSchema("missingSchema")).thenReturn(null);
        JsonNode payload = objectMapper.readTree("{\"name\":\"test\"}");

        assertThrows(CustomException.class, () -> payloadValidation.validatePayload("missingSchema", payload));
    }

    @Test
    void validatePayloadWrapsUnexpectedExceptionAsCustomException() {
        when(schemaCache.getSchema("mySchema")).thenThrow(new RuntimeException("cache failure"));

        assertThrows(CustomException.class, () -> payloadValidation.validatePayload("mySchema", null));
    }
}
