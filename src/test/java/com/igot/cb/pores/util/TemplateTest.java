package com.igot.cb.pores.util;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TemplateTest {

    @Test
    void constructorAndGettersRoundTrip() {
        Map<String, Object> params = new HashMap<>();
        params.put("name", "value");

        Template template = new Template("data-1", "id-1", params);

        assertEquals("data-1", template.getData());
        assertEquals("id-1", template.getId());
        assertEquals(params, template.getParams());
    }

    @Test
    void settersUpdateFields() {
        Template template = new Template(null, null, null);

        template.setData("new-data");
        template.setId("new-id");
        Map<String, Object> params = new HashMap<>();
        params.put("k", "v");
        template.setParams(params);

        assertEquals("new-data", template.getData());
        assertEquals("new-id", template.getId());
        assertEquals(params, template.getParams());
    }

    @Test
    void toStringContainsFieldValues() {
        Map<String, Object> params = new HashMap<>();
        params.put("k", "v");
        Template template = new Template("data-1", "id-1", params);

        String result = template.toString();

        assertTrue(result.contains("data-1"));
        assertTrue(result.contains("id-1"));
        assertTrue(result.contains("Template{"));
    }
}
