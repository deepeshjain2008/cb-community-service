package com.igot.cb.pores.util;

import com.igot.cb.pores.exceptions.CustomException;
import com.networknt.schema.JsonSchema;
import com.networknt.schema.JsonSchemaFactory;
import com.networknt.schema.SpecVersion;
import java.io.InputStream;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import lombok.extern.slf4j.Slf4j;
import org.springframework.core.io.ClassPathResource;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class JsonSchemaCache {

  private final Map<String, JsonSchema> schemaCache = new ConcurrentHashMap<>();

  private JsonSchema loadSchema(String key, String schemaPath) {
    try (InputStream inputStream = new ClassPathResource(schemaPath).getInputStream()) {
      JsonSchema schema = JsonSchemaFactory.getInstance(SpecVersion.VersionFlag.V7)
          .getSchema(inputStream);
      schemaCache.put(key, schema);
      log.info("Successfully loaded schema file from path: {}", schemaPath);
      return schema;
    } catch (Exception e) {
      log.error("Failed to load JSON schema: {}", schemaPath, e);
      throw new CustomException(Constants.ERROR, "Failed to load JSON schema: " + schemaPath,
          HttpStatus.INTERNAL_SERVER_ERROR);
    }
  }

  public JsonSchema getSchema(String schemaKey) {
    JsonSchema jSchema = schemaCache.get(schemaKey);
    if (jSchema == null) {
      return loadSchema(schemaKey, PropertiesCache.getInstance().getProperty(schemaKey));
    }
    return schemaCache.get(schemaKey);
  }

}
