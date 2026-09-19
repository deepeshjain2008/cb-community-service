package com.igot.cb.community.kafka.config;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class ConsumerConfigurationTest {

    private ConsumerConfiguration consumerConfiguration;

    @BeforeEach
    void setUp() {
        consumerConfiguration = new ConsumerConfiguration();
        ReflectionTestUtils.setField(consumerConfiguration, "kafkabootstrapAddress", "localhost:9092");
        ReflectionTestUtils.setField(consumerConfiguration, "kafkaOffsetResetValue", "earliest");
        ReflectionTestUtils.setField(consumerConfiguration, "kafkaMaxPollInterval", 300000);
        ReflectionTestUtils.setField(consumerConfiguration, "kafkaMaxPollRecords", 500);
        ReflectionTestUtils.setField(consumerConfiguration, "kafkaAutoCommitInterval", 1000);
    }

    @Test
    void consumerConfigsContainsAllExpectedProperties() {
        Map<String, Object> configs = consumerConfiguration.consumerConfigs();

        assertEquals("localhost:9092", configs.get(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG));
        assertEquals(true, configs.get(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG));
        assertEquals("1000", configs.get(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG));
        assertEquals(1000, configs.get(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG));
        assertEquals("15000", configs.get(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG));
        assertEquals(StringDeserializer.class, configs.get(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG));
        assertEquals(StringDeserializer.class, configs.get(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG));
        assertEquals("earliest", configs.get(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG));
        assertEquals(300000, configs.get(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG));
        assertEquals(500, configs.get(ConsumerConfig.MAX_POLL_RECORDS_CONFIG));
    }

    @Test
    void consumerFactoryIsCreatedWithConfigs() {
        ConsumerFactory<String, String> factory = consumerConfiguration.consumerFactory();

        assertNotNull(factory);
        assertEquals("localhost:9092",
            factory.getConfigurationProperties().get(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG));
    }

    @Test
    void kafkaListenerContainerFactoryIsConfiguredWithConcurrencyAndPollTimeout() {
        KafkaListenerContainerFactory<ConcurrentMessageListenerContainer<String, String>> factory =
            consumerConfiguration.kafkaListenerContainerFactory();

        assertInstanceOf(ConcurrentKafkaListenerContainerFactory.class, factory);
    }
}
