package com.igot.cb.community.kafka.config;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.test.util.ReflectionTestUtils;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class ProducerConfigurationTest {

    private ProducerConfiguration producerConfiguration;

    @BeforeEach
    void setUp() {
        producerConfiguration = new ProducerConfiguration();
        ReflectionTestUtils.setField(producerConfiguration, "kafkabootstrapAddress", "localhost:9092");
    }

    @Test
    void producerFactoryIsConfiguredWithBootstrapAndSerializers() {
        ProducerFactory<String, String> factory = producerConfiguration.producerFactory();

        assertNotNull(factory);
        assertEquals("localhost:9092",
            factory.getConfigurationProperties().get(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG));
        assertEquals(StringSerializer.class,
            factory.getConfigurationProperties().get(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG));
        assertEquals(StringSerializer.class,
            factory.getConfigurationProperties().get(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG));
    }

    @Test
    void kafkaTemplateWrapsProducerFactory() {
        KafkaTemplate<String, String> template = producerConfiguration.kafkaTemplate();

        assertNotNull(template);
        assertNotNull(template.getProducerFactory());
    }
}
