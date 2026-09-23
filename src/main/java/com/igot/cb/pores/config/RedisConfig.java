package com.igot.cb.pores.config;



import com.igot.cb.pores.elasticsearch.dto.SearchResult;
import com.igot.cb.pores.util.Constants;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.cache.annotation.EnableCaching;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.RedisStandaloneConfiguration;
import org.springframework.data.redis.connection.lettuce.LettuceClientConfiguration;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.connection.lettuce.LettucePoolingClientConfiguration;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.serializer.GenericJackson2JsonRedisSerializer;
import org.springframework.data.redis.serializer.StringRedisSerializer;

import java.time.Duration;

@Configuration
@EnableCaching
public class RedisConfig {
  
  @Value("${spring.redis.host}")
  private String redisHost;

  @Value("${spring.redis.port}")
  private int redisPort;

  @Value("${spring.redis.data.host}")
  private String redisDataHost;

  @Value("${spring.redis.data.port}")
  private int redisDataPort;

  private static final long redisTimeout = 60000;

  @Bean(name = Constants.REDIS_CONNECTION_FACTORY)
  public RedisConnectionFactory redisConnectionFactory() {
    RedisStandaloneConfiguration config = new RedisStandaloneConfiguration();
    config.setHostName(redisHost);
    config.setPort(redisPort);
    config.setDatabase(0);
    LettuceClientConfiguration clientConfig = LettucePoolingClientConfiguration.builder()
            .commandTimeout(Duration.ofMillis(redisTimeout))
            .poolConfig(buildPoolConfig())
            .build();
    return new LettuceConnectionFactory(config, clientConfig);
  }

  // Redis connection for data
  @Bean(name = Constants.REDIS_DATA_CONNECTION_FACTORY)
  public RedisConnectionFactory redisDataConnectionFactory() {
    RedisStandaloneConfiguration config = new RedisStandaloneConfiguration();
    config.setHostName(redisDataHost);
    config.setPort(redisDataPort);
    config.setDatabase(0);
    LettuceClientConfiguration clientConfig = LettucePoolingClientConfiguration.builder()
            .commandTimeout(Duration.ofMillis(redisTimeout))
            .poolConfig(buildPoolConfig())
            .build();
    return new LettuceConnectionFactory(config, clientConfig);
  }

  // RedisTemplate for general string caching
  @Bean
  public RedisTemplate<String, String> redisTemplate(
          @Qualifier(Constants.REDIS_CONNECTION_FACTORY) RedisConnectionFactory redisConnectionFactory) {
    RedisTemplate<String, String> template = new RedisTemplate<>();
    template.setConnectionFactory(redisConnectionFactory);
    template.setKeySerializer(new StringRedisSerializer());
    template.setValueSerializer(new StringRedisSerializer());
    template.setHashKeySerializer(new StringRedisSerializer());
    template.setHashValueSerializer(new StringRedisSerializer());
    return template;
  }

  // RedisTemplate for data Redis
  @Bean(name = Constants.REDIS_DATA_TEMPLATE)
  public RedisTemplate<String, String> redisDataTemplate(
          @Qualifier(Constants.REDIS_DATA_CONNECTION_FACTORY) RedisConnectionFactory redisDataConnectionFactory) {
    RedisTemplate<String, String> template = new RedisTemplate<>();
    template.setConnectionFactory(redisDataConnectionFactory);
    template.setKeySerializer(new StringRedisSerializer());
    template.setValueSerializer(new StringRedisSerializer());
    return template;
  }

  // RedisTemplate for SearchResult object
  @Bean(name = Constants.SEARCH_RESULT_REDIS_TEMPLATE)
  public RedisTemplate<String, SearchResult> searchResultRedisTemplate(
          @Qualifier(Constants.REDIS_CONNECTION_FACTORY) RedisConnectionFactory redisConnectionFactory) {
    RedisTemplate<String, SearchResult> template = new RedisTemplate<>();
    template.setConnectionFactory(redisConnectionFactory);
    template.setKeySerializer(new StringRedisSerializer());
    return template;
  }

  private GenericObjectPoolConfig<?> buildPoolConfig() {
    GenericObjectPoolConfig<?> poolConfig = new GenericObjectPoolConfig<>();
    poolConfig.setMaxTotal(3000);
    poolConfig.setMaxIdle(128);
    poolConfig.setMinIdle(100);
    poolConfig.setMaxWait(Duration.ofMillis(5000));
    return poolConfig;
  }

  @Bean(name = Constants.REDIS_OBJECT_TEMPLATE)
  public RedisTemplate<String, Object> redisObjectTemplate(RedisConnectionFactory redisConnectionFactory) {
    RedisTemplate<String, Object> template = new RedisTemplate<>();
    template.setConnectionFactory(redisConnectionFactory);
    template.setKeySerializer(new StringRedisSerializer());
    template.setValueSerializer(new GenericJackson2JsonRedisSerializer());
    return template;
  }
}
