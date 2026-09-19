package com.igot.cb.pores.util;

import com.igot.cb.pores.exceptions.CustomException;
import com.networknt.schema.JsonSchema;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

class JsonSchemaCacheTest {

    private final JsonSchemaCache jsonSchemaCache = new JsonSchemaCache();

    @Test
    void getSchemaLoadsRealSchemaFileFromClasspath() {
        JsonSchema schema = jsonSchemaCache.getSchema(Constants.PAYLOAD_VALIDATION_FILE);

        assertNotNull(schema);
    }

    @Test
    void getSchemaReturnsCachedInstanceOnSecondCall() {
        JsonSchema first = jsonSchemaCache.getSchema(Constants.CATEGORY_PAYLOAD_VALIDATION_FILE);
        JsonSchema second = jsonSchemaCache.getSchema(Constants.CATEGORY_PAYLOAD_VALIDATION_FILE);

        assertSame(first, second);
    }

    @Test
    void getSchemaThrowsCustomExceptionForMissingResource() {
        assertThrows(CustomException.class, () -> jsonSchemaCache.getSchema("/does/not/exist.json"));
    }
}
