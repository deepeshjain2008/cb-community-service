package com.igot.cb.pores.cache;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.igot.cb.pores.util.CbServerProperties;
import com.igot.cb.pores.util.Constants;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.Cursor;
import org.springframework.data.redis.core.HashOperations;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.ScanOptions;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class CacheService {

  private final RedisTemplate<String, String> redisTemplate;
  private final RedisTemplate<String, String> redisDataTemplate;
  private final ObjectMapper objectMapper;

  @Value("${spring.redis.cacheTtl}")
  private long cacheTtl;

  private final CbServerProperties properties;

  @Autowired
  public CacheService(RedisTemplate<String, String> redisTemplate,
      @Qualifier(Constants.REDIS_DATA_TEMPLATE) RedisTemplate<String, String> redisDataTemplate,
      ObjectMapper objectMapper, CbServerProperties properties) {
    this.redisTemplate = redisTemplate;
    this.redisDataTemplate = redisDataTemplate;
    this.objectMapper = objectMapper;
    this.properties = properties;
  }

  public void putCache(String key, Object object) {
    try {
      String data = objectMapper.writeValueAsString(object);
      redisTemplate.opsForValue().set(Constants.REDIS_KEY_PREFIX + key, data, cacheTtl, TimeUnit.SECONDS);
    } catch (Exception e) {
      log.error("Error while putting data in Redis cache: {}", e.getMessage(), e);
    }
  }

  public String getCache(String key) {
    try {
      return redisTemplate.opsForValue().get(Constants.REDIS_KEY_PREFIX + key);
    } catch (Exception e) {
      log.error("Error while getting data from Redis cache: {}", e.getMessage(), e);
      return null;
    }
  }

  public Long deleteCache(String key) {
    try {
      boolean result = redisTemplate.delete(Constants.REDIS_KEY_PREFIX + key);
      if (result) {
        log.info("Field deleted successfully from key {}.", key);
      } else {
        log.warn("Field not found in key {}.", key);
      }
    } catch (Exception e) {
      log.error("Error while deleting key {} from Redis: {}", key, e.getMessage(), e);
    }
    return null;
  }

  public void addUsersToHash(String key, Set<String> userIds) {
    try {
      // Prepare the user map
      Map<String, String> userMap = new HashMap<>();
      for (String userId : userIds) {
        userMap.put(userId, userId); // Using userId as both field and value
      }

      // Add each field to the hash only if it does not exist
      HashOperations<String, String, String> hashOps = redisTemplate.opsForHash();
      for (Map.Entry<String, String> entry : userMap.entrySet()) {
        Boolean isAbsent = hashOps.putIfAbsent(key, entry.getKey(), entry.getValue());
        // Optional: track how many were actually added
      }
      redisTemplate.expire(key, properties.getRedisCommunityUserDataTtlSeconds(), TimeUnit.SECONDS);
    } catch (Exception e) {
      log.error("Error while adding users to Redis Hash: {}", e.getMessage(), e);
    }
  }



  public List<String> getPaginatedUsersFromHash(String key, int offset, int limit) {
    try {
      HashOperations<String, String, String> hashOps = redisTemplate.opsForHash();
      
      // Use HSCAN to iterate through hash without loading all entries into memory
      List<String> userIdList = new ArrayList<>();
      ScanOptions scanOptions = ScanOptions.scanOptions().count(properties.getRedisScanCountSize()).build();
      
      try (Cursor<Map.Entry<String, String>> cursor = hashOps.scan(key, scanOptions)) {
        while (cursor.hasNext()) {
          userIdList.add(cursor.next().getKey());
        }
      }

      // Sort users if needed
      Collections.sort(userIdList);

      // Calculate the starting and ending indices
      int startIndex = offset * limit;
      int endIndex = Math.min(startIndex + limit, userIdList.size());

      // Apply pagination
      if (startIndex < userIdList.size()) {
        return new ArrayList<>(userIdList.subList(startIndex, endIndex));
      } else {
        return Collections.emptyList();
      }
    } catch (Exception e) {
      log.error("Error while fetching paginated users from Redis Hash: {}", e.getMessage(), e);
      return Collections.emptyList();
    }
  }


  public Long getListSize(String key) {
    try {
      HashOperations<String, String, String> hashOps = redisTemplate.opsForHash();
      return hashOps.size(key);
    } catch (Exception e) {
      log.error("Error while fetching hash size from Redis: {}", e.getMessage(), e);
      return null;
    }
  }

  public void deleteUserFromHash(String key, String field) {
    try {
      HashOperations<String, String, String> hashOps = redisTemplate.opsForHash();
      Long fieldsRemoved = hashOps.delete(key, field);

      if (fieldsRemoved != null && fieldsRemoved > 0) {
        log.info("Field '{}' removed from hash '{}'", field, key);
      } else {
        log.warn("Field '{}' does not exist in hash '{}'", field, key);
      }
    } catch (Exception e) {
      log.error("Error while deleting field from Redis Hash: {}", e.getMessage(), e);
    }
  }

  public List<Object> hget(List<String> keys) {
    List<Object> resultList = new ArrayList<>();
    try {
      for (String key : keys) {
        String value = redisDataTemplate.opsForValue().get(key);
        resultList.add(value);
      }
    } catch (Exception e) {
      log.error("Error while fetching data from Redis: {}", e.getMessage(), e);
    }
    return resultList;
  }
}
