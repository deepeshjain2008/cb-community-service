package com.igot.cb.pores.cache;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.igot.cb.pores.util.CbServerProperties;
import com.igot.cb.pores.util.Constants;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.redis.core.Cursor;
import org.springframework.data.redis.core.HashOperations;
import org.springframework.data.redis.core.ValueOperations;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.AbstractMap;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class CacheServiceTest {

    @Mock
    private org.springframework.data.redis.core.RedisTemplate<String, String> redisTemplate;

    @Mock
    private org.springframework.data.redis.core.RedisTemplate<String, String> redisDataTemplate;

    @Mock
    private ValueOperations<String, String> valueOperations;

    private CacheService cacheService;
    private CbServerProperties properties;

    @BeforeEach
    void setUp() {
        properties = new CbServerProperties();
        properties.setRedisScanCountSize(10);
        properties.setRedisCommunityUserDataTtlSeconds(3600L);
        cacheService = new CacheService(redisTemplate, redisDataTemplate, new ObjectMapper(), properties);
        ReflectionTestUtils.setField(cacheService, "cacheTtl", 60L);
    }

    @Test
    void putCacheStoresSerializedValue() {
        lenient().when(redisTemplate.opsForValue()).thenReturn(valueOperations);

        cacheService.putCache("key1", Map.of("a", "b"));

        verify(valueOperations).set(eq(Constants.REDIS_KEY_PREFIX + "key1"), anyString(), eq(60L), eq(TimeUnit.SECONDS));
    }

    @Test
    void putCacheHandlesExceptionGracefully() {
        when(redisTemplate.opsForValue()).thenThrow(new RuntimeException("redis down"));

        assertDoesNotThrow(() -> cacheService.putCache("key1", Map.of("a", "b")));
    }

    @Test
    void getCacheReturnsValue() {
        when(redisTemplate.opsForValue()).thenReturn(valueOperations);
        when(valueOperations.get(Constants.REDIS_KEY_PREFIX + "key1")).thenReturn("cached-value");

        String result = cacheService.getCache("key1");

        assertEquals("cached-value", result);
    }

    @Test
    void getCacheReturnsNullOnException() {
        when(redisTemplate.opsForValue()).thenThrow(new RuntimeException("redis down"));

        String result = cacheService.getCache("key1");

        assertNull(result);
    }

    @Test
    void deleteCacheLogsSuccessWhenDeleted() {
        when(redisTemplate.delete(Constants.REDIS_KEY_PREFIX + "key1")).thenReturn(true);

        Long result = cacheService.deleteCache("key1");

        assertNull(result);
    }

    @Test
    void deleteCacheLogsWarningWhenNotFound() {
        when(redisTemplate.delete(Constants.REDIS_KEY_PREFIX + "key1")).thenReturn(false);

        Long result = cacheService.deleteCache("key1");

        assertNull(result);
    }

    @Test
    void deleteCacheHandlesException() {
        when(redisTemplate.delete(anyString())).thenThrow(new RuntimeException("redis down"));

        Long result = cacheService.deleteCache("key1");

        assertNull(result);
    }

    @Test
    void addUsersToHashPutsEachUserAndSetsExpiry() {
        HashOperations<String, String, String> hashOps = mock(HashOperations.class);
        when(redisTemplate.<String, String>opsForHash()).thenReturn(hashOps);
        Set<String> userIds = new HashSet<>(Arrays.asList("u1", "u2"));

        cacheService.addUsersToHash("hashKey", userIds);

        verify(hashOps, times(2)).putIfAbsent(eq("hashKey"), anyString(), anyString());
        verify(redisTemplate).expire("hashKey", 3600L, TimeUnit.SECONDS);
    }

    @Test
    void addUsersToHashHandlesException() {
        when(redisTemplate.<String, String>opsForHash()).thenThrow(new RuntimeException("redis down"));

        assertDoesNotThrow(() -> cacheService.addUsersToHash("hashKey", new HashSet<>(List.of("u1"))));

        verify(redisTemplate, never()).expire(anyString(), anyLong(), any(TimeUnit.class));
    }

    @Test
    void getPaginatedUsersFromHashReturnsSortedSubset() {
        HashOperations<String, String, String> hashOps = mock(HashOperations.class);
        when(redisTemplate.<String, String>opsForHash()).thenReturn(hashOps);
        Cursor<Map.Entry<String, String>> cursor = mock(Cursor.class);
        Iterator<Map.Entry<String, String>> entries = Arrays.<Map.Entry<String, String>>asList(
                new AbstractMap.SimpleEntry<>("u3", "u3"),
                new AbstractMap.SimpleEntry<>("u1", "u1"),
                new AbstractMap.SimpleEntry<>("u2", "u2")
        ).iterator();
        when(cursor.hasNext()).thenAnswer(inv -> entries.hasNext());
        when(cursor.next()).thenAnswer(inv -> entries.next());
        when(hashOps.scan(eq("hashKey"), any())).thenReturn(cursor);

        List<String> result = cacheService.getPaginatedUsersFromHash("hashKey", 0, 2);

        assertEquals(Arrays.asList("u1", "u2"), result);
    }

    @Test
    void getPaginatedUsersFromHashReturnsEmptyWhenOffsetBeyondSize() {
        HashOperations<String, String, String> hashOps = mock(HashOperations.class);
        when(redisTemplate.<String, String>opsForHash()).thenReturn(hashOps);
        Cursor<Map.Entry<String, String>> cursor = mock(Cursor.class);
        Iterator<Map.Entry<String, String>> entries = List.<Map.Entry<String, String>>of(
                new AbstractMap.SimpleEntry<>("u1", "u1")
        ).iterator();
        when(cursor.hasNext()).thenAnswer(inv -> entries.hasNext());
        when(cursor.next()).thenAnswer(inv -> entries.next());
        when(hashOps.scan(eq("hashKey"), any())).thenReturn(cursor);

        List<String> result = cacheService.getPaginatedUsersFromHash("hashKey", 5, 2);

        assertTrue(result.isEmpty());
    }

    @Test
    void getPaginatedUsersFromHashHandlesException() {
        when(redisTemplate.<String, String>opsForHash()).thenThrow(new RuntimeException("redis down"));

        List<String> result = cacheService.getPaginatedUsersFromHash("hashKey", 0, 2);

        assertTrue(result.isEmpty());
    }

    @Test
    void getListSizeReturnsHashSize() {
        HashOperations<String, String, String> hashOps = mock(HashOperations.class);
        when(redisTemplate.<String, String>opsForHash()).thenReturn(hashOps);
        when(hashOps.size("hashKey")).thenReturn(5L);

        Long result = cacheService.getListSize("hashKey");

        assertEquals(5L, result);
    }

    @Test
    void getListSizeReturnsNullOnException() {
        when(redisTemplate.<String, String>opsForHash()).thenThrow(new RuntimeException("redis down"));

        Long result = cacheService.getListSize("hashKey");

        assertNull(result);
    }

    @Test
    void deleteUserFromHashLogsSuccessWhenRemoved() {
        HashOperations<String, String, String> hashOps = mock(HashOperations.class);
        when(redisTemplate.<String, String>opsForHash()).thenReturn(hashOps);
        when(hashOps.delete("hashKey", "field1")).thenReturn(1L);

        cacheService.deleteUserFromHash("hashKey", "field1");

        verify(hashOps).delete("hashKey", "field1");
    }

    @Test
    void deleteUserFromHashLogsWarningWhenFieldMissing() {
        HashOperations<String, String, String> hashOps = mock(HashOperations.class);
        when(redisTemplate.<String, String>opsForHash()).thenReturn(hashOps);
        when(hashOps.delete("hashKey", "field1")).thenReturn(0L);

        cacheService.deleteUserFromHash("hashKey", "field1");

        verify(hashOps).delete("hashKey", "field1");
    }

    @Test
    void deleteUserFromHashLogsWarningWhenResultNull() {
        HashOperations<String, String, String> hashOps = mock(HashOperations.class);
        when(redisTemplate.<String, String>opsForHash()).thenReturn(hashOps);
        when(hashOps.delete("hashKey", "field1")).thenReturn(null);

        cacheService.deleteUserFromHash("hashKey", "field1");

        verify(hashOps).delete("hashKey", "field1");
    }

    @Test
    void deleteUserFromHashHandlesException() {
        when(redisTemplate.<String, String>opsForHash()).thenThrow(new RuntimeException("redis down"));

        assertDoesNotThrow(() -> cacheService.deleteUserFromHash("hashKey", "field1"));
    }

    @Test
    void hgetReturnsValuesForEachKey() {
        ValueOperations<String, String> dataOps = mock(ValueOperations.class);
        when(redisDataTemplate.opsForValue()).thenReturn(dataOps);
        when(dataOps.get("k1")).thenReturn("v1");
        when(dataOps.get("k2")).thenReturn("v2");

        List<Object> result = cacheService.hget(Arrays.asList("k1", "k2"));

        assertEquals(Arrays.asList("v1", "v2"), result);
    }

    @Test
    void hgetHandlesExceptionAndReturnsPartialResults() {
        when(redisDataTemplate.opsForValue()).thenThrow(new RuntimeException("redis down"));

        List<Object> result = cacheService.hget(Arrays.asList("k1"));

        assertTrue(result.isEmpty());
    }
}
