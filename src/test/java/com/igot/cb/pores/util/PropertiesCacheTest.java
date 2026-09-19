package com.igot.cb.pores.util;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;

class PropertiesCacheTest {

    private static final String TEST_KEY = "properties.cache.test.key";

    @AfterEach
    void cleanup() throws Exception {
        Properties props = configProp();
        props.remove(TEST_KEY);
    }

    private Properties configProp() throws Exception {
        Field field = PropertiesCache.class.getDeclaredField("configProp");
        field.setAccessible(true);
        return (Properties) field.get(PropertiesCache.getInstance());
    }

    @Test
    void getInstanceReturnsSameSingleton() {
        PropertiesCache first = PropertiesCache.getInstance();
        PropertiesCache second = PropertiesCache.getInstance();

        assertSame(first, second);
    }

    @Test
    void getPropertyReturnsConfiguredValueWhenPresent() {
        PropertiesCache cache = PropertiesCache.getInstance();

        String value = cache.getProperty(Constants.SSO_URL);

        assertEquals("https://portal.dev.karmayogibharat.net/auth/", value);
    }

    @Test
    void getPropertyReturnsKeyItselfWhenNotConfigured() {
        PropertiesCache cache = PropertiesCache.getInstance();

        String value = cache.getProperty("no.such.property.key.exists");

        assertEquals("no.such.property.key.exists", value);
    }

    @Test
    void readPropertyReturnsConfiguredValueWhenPresent() {
        PropertiesCache cache = PropertiesCache.getInstance();

        String value = cache.readProperty(Constants.SSO_REALM);

        assertEquals("sunbird", value);
    }

    @Test
    void readPropertyReturnsNullWhenNotConfigured() {
        PropertiesCache cache = PropertiesCache.getInstance();

        String value = cache.readProperty("no.such.property.key.exists");

        assertNull(value);
    }

    @Test
    void getPropertyReturnsConfiguredValueEvenWhenBlank() throws Exception {
        Properties props = configProp();
        props.setProperty(TEST_KEY, "");
        PropertiesCache cache = PropertiesCache.getInstance();

        String value = cache.getProperty(TEST_KEY);

        assertEquals("", value);
    }
}
