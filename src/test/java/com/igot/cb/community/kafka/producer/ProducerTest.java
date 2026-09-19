package com.igot.cb.community.kafka.producer;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;

import java.util.HashMap;
import java.util.Map;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.contains;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
class ProducerTest {

    @Mock
    private KafkaTemplate<String, String> kafkaTemplate;

    private Producer producer;

    @BeforeEach
    void setUp() {
        producer = new Producer(kafkaTemplate);
    }

    @Test
    void pushSerializesValueAndSendsToTopic() {
        Map<String, Object> value = new HashMap<>();
        value.put("id", "123");

        producer.push("my-topic", value);

        verify(kafkaTemplate).send(eq("my-topic"), contains("\"id\":\"123\""));
    }

    @Test
    void pushSkipsSendWhenSerializationFails() {
        producer.push("my-topic", new BadBean());

        verify(kafkaTemplate, never()).send(anyString(), anyString());
    }

    static class BadBean {
        public String getValue() {
            throw new RuntimeException("cannot serialize");
        }
    }
}
