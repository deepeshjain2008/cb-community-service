package com.igot.cb.pores.config;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.serializer.GenericJackson2JsonRedisSerializer;
import org.springframework.data.redis.serializer.StringRedisSerializer;
import org.springframework.test.util.ReflectionTestUtils;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;

class RedisConfigTest {

    private RedisConfig redisConfig;

    @BeforeEach
    void setUp() {
        redisConfig = new RedisConfig();
        ReflectionTestUtils.setField(redisConfig, "redisHost", "localhost");
        ReflectionTestUtils.setField(redisConfig, "redisPort", 6379);
        ReflectionTestUtils.setField(redisConfig, "redisDataHost", "data-host");
        ReflectionTestUtils.setField(redisConfig, "redisDataPort", 6380);
    }

    @Test
    void redisConnectionFactoryUsesConfiguredHostAndPort() {
        RedisConnectionFactory factory = redisConfig.redisConnectionFactory();

        assertInstanceOf(LettuceConnectionFactory.class, factory);
        LettuceConnectionFactory lettuceFactory = (LettuceConnectionFactory) factory;
        assertEquals("localhost", lettuceFactory.getHostName());
        assertEquals(6379, lettuceFactory.getPort());
    }

    @Test
    void redisDataConnectionFactoryUsesConfiguredHostAndPort() {
        RedisConnectionFactory factory = redisConfig.redisDataConnectionFactory();

        assertInstanceOf(LettuceConnectionFactory.class, factory);
        LettuceConnectionFactory lettuceFactory = (LettuceConnectionFactory) factory;
        assertEquals("data-host", lettuceFactory.getHostName());
        assertEquals(6380, lettuceFactory.getPort());
    }

    @Test
    void redisTemplateUsesStringSerializersAndFactory() {
        RedisConnectionFactory factory = redisConfig.redisConnectionFactory();

        RedisTemplate<String, String> template = redisConfig.redisTemplate(factory);

        assertSame(factory, template.getConnectionFactory());
        assertInstanceOf(StringRedisSerializer.class, template.getKeySerializer());
        assertInstanceOf(StringRedisSerializer.class, template.getValueSerializer());
        assertInstanceOf(StringRedisSerializer.class, template.getHashKeySerializer());
        assertInstanceOf(StringRedisSerializer.class, template.getHashValueSerializer());
    }

    @Test
    void redisDataTemplateUsesStringSerializersAndFactory() {
        RedisConnectionFactory factory = redisConfig.redisDataConnectionFactory();

        RedisTemplate<String, String> template = redisConfig.redisDataTemplate(factory);

        assertSame(factory, template.getConnectionFactory());
        assertInstanceOf(StringRedisSerializer.class, template.getKeySerializer());
        assertInstanceOf(StringRedisSerializer.class, template.getValueSerializer());
    }

    @Test
    void searchResultRedisTemplateUsesStringKeySerializerAndFactory() {
        RedisConnectionFactory factory = redisConfig.redisConnectionFactory();

        RedisTemplate<String, com.igot.cb.pores.elasticsearch.dto.SearchResult> template =
            redisConfig.searchResultRedisTemplate(factory);

        assertSame(factory, template.getConnectionFactory());
        assertInstanceOf(StringRedisSerializer.class, template.getKeySerializer());
    }

    @Test
    void redisObjectTemplateUsesJsonValueSerializerAndFactory() {
        RedisConnectionFactory factory = redisConfig.redisConnectionFactory();

        RedisTemplate<String, Object> template = redisConfig.redisObjectTemplate(factory);

        assertSame(factory, template.getConnectionFactory());
        assertInstanceOf(StringRedisSerializer.class, template.getKeySerializer());
        assertInstanceOf(GenericJackson2JsonRedisSerializer.class, template.getValueSerializer());
    }

    @Test
    void allBeansAreCreatedNonNull() {
        assertNotNull(redisConfig.redisConnectionFactory());
        assertNotNull(redisConfig.redisDataConnectionFactory());
    }
}
