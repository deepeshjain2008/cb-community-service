package com.igot.cb.pores.util;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ConfigTest {

    @Test
    void settersAndGettersRoundTrip() {
        Config config = new Config();

        config.setSender("sender@example.com");
        config.setTopic("topic-1");
        config.setOtp("123456");
        config.setSubject("subject-1");

        assertEquals("sender@example.com", config.getSender());
        assertEquals("topic-1", config.getTopic());
        assertEquals("123456", config.getOtp());
        assertEquals("subject-1", config.getSubject());
    }
}
