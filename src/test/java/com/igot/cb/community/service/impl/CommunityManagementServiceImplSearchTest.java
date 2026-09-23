package com.igot.cb.community.service.impl;

import co.elastic.clients.elasticsearch._types.aggregations.Aggregate;
import co.elastic.clients.elasticsearch._types.aggregations.Buckets;
import co.elastic.clients.elasticsearch._types.aggregations.StringTermsBucket;
import co.elastic.clients.elasticsearch.core.SearchRequest;
import co.elastic.clients.elasticsearch.core.SearchResponse;
import co.elastic.clients.elasticsearch.core.search.Hit;
import co.elastic.clients.elasticsearch.core.search.HitsMetadata;
import co.elastic.clients.elasticsearch.core.search.TotalHitsRelation;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.igot.cb.authentication.util.AccessTokenValidator;
import com.igot.cb.community.kafka.producer.Producer;
import com.igot.cb.community.repository.CommunityCategoryRepository;
import com.igot.cb.community.repository.CommunityEngagementRepository;
import com.igot.cb.community.service.NotificationService;
import com.igot.cb.community.service.UserService;
import com.igot.cb.pores.cache.CacheService;
import com.igot.cb.pores.elasticsearch.dto.SearchCriteria;
import com.igot.cb.pores.elasticsearch.dto.SearchResult;
import com.igot.cb.pores.elasticsearch.service.EsUtilService;
import com.igot.cb.pores.exceptions.CustomException;
import com.igot.cb.pores.util.ApiResponse;
import com.igot.cb.pores.util.CbServerProperties;
import com.igot.cb.pores.util.Constants;
import com.igot.cb.pores.util.FileProcessService;
import com.igot.cb.pores.util.PayloadValidation;
import com.igot.cb.transactional.cassandrautils.CassandraOperation;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.ValueOperations;
import org.springframework.http.HttpStatus;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class CommunityManagementServiceImplSearchTest {

    @Mock private EsUtilService esUtilService;
    @Mock private CacheService cacheService;
    @Mock private PayloadValidation payloadValidation;
    @Mock private CommunityEngagementRepository communityEngagementRepository;
    @Mock private AccessTokenValidator accessTokenValidator;
    @Mock private CassandraOperation cassandraOperation;
    @Mock private RedisTemplate<String, SearchResult> redisTemplate;
    @Mock private ValueOperations<String, SearchResult> valueOperations;
    @Mock private CommunityCategoryRepository categoryRepository;
    @Mock private Producer producer;
    @Mock private UserService userService;
    @Mock private NotificationService notificationService;
    @Mock private FileProcessService fileProcessService;

    private CommunityManagementServiceImpl service;
    private final ObjectMapper objectMapper = new ObjectMapper();

    @BeforeEach
    void setUp() {
        CbServerProperties cbServerProperties = new CbServerProperties();
        cbServerProperties.setJwtSecretKey("test-secret-key");
        cbServerProperties.setSearchResultRedisTtl(60L);
        service = new CommunityManagementServiceImpl(esUtilService, cacheService, objectMapper,
            cbServerProperties, payloadValidation, communityEngagementRepository, accessTokenValidator,
            cassandraOperation, redisTemplate, categoryRepository, producer, userService,
            notificationService, fileProcessService);
        ReflectionTestUtils.setField(service, "communityIndex", "community-index");
        ReflectionTestUtils.setField(service, "communityCategoryIndex", "community-category-index");
        ReflectionTestUtils.setField(service, "noOfPopularCommunities", 10);
        lenient().when(redisTemplate.opsForValue()).thenReturn(valueOperations);
    }

    private SearchCriteria criteria() {
        return new SearchCriteria();
    }

    private SearchResult emptyResult() {
        return new SearchResult(objectMapper.createArrayNode(), Map.of(), 0L, List.of());
    }

    private SearchResult resultWithData() {
        ObjectNode doc = objectMapper.createObjectNode();
        doc.put(Constants.CREATED_BY, "u1");
        doc.put(Constants.ORD_ID, "org-1");
        ArrayNode data = objectMapper.createArrayNode();
        data.add(doc);
        return new SearchResult(data, Map.of(), 1L, List.of());
    }

    // ---- searchCommunity ----

    @Test
    void searchCommunityReturnsFromRedisWhenNotOverridingCache() {
        when(valueOperations.get(any(String.class))).thenReturn(resultWithData());

        ApiResponse response = service.searchCommunity(criteria());

        assertEquals(HttpStatus.OK, response.getResponseCode());
    }

    @Test
    void searchCommunityReturnsBadRequestWhenSearchStringTooShort() {
        when(valueOperations.get(any(String.class))).thenReturn(null);
        SearchCriteria criteria = criteria();
        criteria.setSearchString("a");

        ApiResponse response = service.searchCommunity(criteria);

        assertEquals(HttpStatus.BAD_REQUEST, response.getResponseCode());
    }

    @Test
    void searchCommunityFetchesFromEsAndEnrichesOrgInfoWhenCacheMiss() {
        when(valueOperations.get(any(String.class))).thenReturn(null);
        when(esUtilService.searchDocuments(anyString(), any(SearchCriteria.class))).thenReturn(resultWithData());
        when(cacheService.hget(any(List.class))).thenAnswer(inv -> {
            List<?> keys = inv.getArgument(0);
            return new java.util.ArrayList<>(java.util.Collections.nCopies(keys.size(), null));
        });
        when(cassandraOperation.getRecordsByPropertiesWithoutFiltering(
            any(String.class), any(String.class), any(Map.class), any(List.class), any()))
            .thenReturn(List.of());

        ApiResponse response = service.searchCommunity(criteria());

        assertEquals(HttpStatus.OK, response.getResponseCode());
    }

    @Test
    void searchCommunityWithOverrideCacheSkipsRedisLookup() {
        when(esUtilService.searchDocuments(anyString(), any(SearchCriteria.class))).thenReturn(emptyResult());
        SearchCriteria criteria = criteria();
        criteria.setOverrideCache(true);

        ApiResponse response = service.searchCommunity(criteria);

        assertEquals(HttpStatus.OK, response.getResponseCode());
    }

    @Test
    void searchCommunityThrowsCustomExceptionOnUnexpectedError() {
        when(valueOperations.get(any(String.class))).thenThrow(new RuntimeException("redis down"));

        SearchCriteria criteria = criteria();
        assertThrows(CustomException.class, () -> service.searchCommunity(criteria));
    }

    // ---- searchTopic ----

    @Test
    void searchTopicReturnsFromRedisCache() {
        when(valueOperations.get(any(String.class))).thenReturn(resultWithData());

        ApiResponse response = service.searchTopic(criteria());

        assertEquals(HttpStatus.OK, response.getResponseCode());
    }

    @Test
    void searchTopicReturnsBadRequestWhenSearchStringTooShort() {
        when(valueOperations.get(any(String.class))).thenReturn(null);
        SearchCriteria criteria = criteria();
        criteria.setSearchString("a");

        ApiResponse response = service.searchTopic(criteria);

        assertEquals(HttpStatus.BAD_REQUEST, response.getResponseCode());
    }

    @Test
    void searchTopicFetchesFromEsWhenCacheMiss() {
        when(valueOperations.get(any(String.class))).thenReturn(null);
        when(esUtilService.searchDocuments(anyString(), any(SearchCriteria.class))).thenReturn(emptyResult());

        ApiResponse response = service.searchTopic(criteria());

        assertEquals(HttpStatus.OK, response.getResponseCode());
    }

    @Test
    void searchTopicThrowsCustomExceptionOnUnexpectedError() {
        when(valueOperations.get(any(String.class))).thenThrow(new RuntimeException("redis down"));

        SearchCriteria criteria = criteria();
        assertThrows(CustomException.class, () -> service.searchTopic(criteria));
    }

    // ---- searchCommunityFromPrimary ----

    @Test
    void searchCommunityFromPrimaryReturnsBadRequestWhenSearchStringTooShort() {
        SearchCriteria criteria = criteria();
        criteria.setSearchString("a");

        ApiResponse response = service.searchCommunityFromPrimary(criteria);

        assertEquals(HttpStatus.BAD_REQUEST, response.getResponseCode());
    }

    @Test
    void searchCommunityFromPrimaryReturnsSuccessWithoutEnrichmentWhenNoData() {
        when(esUtilService.searchDocuments(eq(Constants.INDEX_NAME), any(SearchCriteria.class)))
            .thenReturn(emptyResult());

        ApiResponse response = service.searchCommunityFromPrimary(criteria());

        assertEquals(HttpStatus.OK, response.getResponseCode());
    }

    @Test
    void searchCommunityFromPrimaryEnrichesCreatorInfoWhenDataPresent() {
        when(esUtilService.searchDocuments(eq(Constants.INDEX_NAME), any(SearchCriteria.class)))
            .thenReturn(resultWithData());
        when(cacheService.hget(any(List.class))).thenAnswer(inv -> {
            List<?> keys = inv.getArgument(0);
            return new java.util.ArrayList<>(java.util.Collections.nCopies(keys.size(), null));
        });

        ApiResponse response = service.searchCommunityFromPrimary(criteria());

        assertEquals(HttpStatus.OK, response.getResponseCode());
    }

    @Test
    void searchCommunityFromPrimaryReturnsBadRequestOnRuntimeException() {
        when(esUtilService.searchDocuments(eq(Constants.INDEX_NAME), any(SearchCriteria.class)))
            .thenThrow(new RuntimeException("bad query"));

        ApiResponse response = service.searchCommunityFromPrimary(criteria());

        assertEquals(HttpStatus.BAD_REQUEST, response.getResponseCode());
    }

    // ---- getPopularCommunitiesByField ----

    @Test
    void getPopularCommunitiesByFieldReturnsBadRequestWhenFieldMissing() {
        ApiResponse response = service.getPopularCommunitiesByField(new HashMap<>());

        assertEquals(HttpStatus.BAD_REQUEST, response.getResponseCode());
    }

    @Test
    void getPopularCommunitiesByFieldReturnsSuccessWithNoAggregations() {
        Map<String, Object> payload = new HashMap<>();
        payload.put(Constants.FIELD, "communityName");

        SearchResponse searchResponse = mock(SearchResponse.class);
        HitsMetadata hitsMetadata = mock(HitsMetadata.class);
        when(hitsMetadata.hits()).thenReturn(List.of());
        when(searchResponse.hits()).thenReturn(hitsMetadata);
        when(searchResponse.aggregations()).thenReturn(null);
        when(esUtilService.popularCommunities(any(SearchRequest.class), any()))
            .thenReturn(searchResponse);

        ApiResponse response = service.getPopularCommunitiesByField(payload);

        assertEquals(HttpStatus.OK, response.getResponseCode());
    }

    @Test
    void getPopularCommunitiesByFieldUsesOffsetAndLimitWhenProvided() {
        Map<String, Object> payload = new HashMap<>();
        payload.put(Constants.FIELD, "communityName");
        payload.put(Constants.OFFSET, 1);
        payload.put(Constants.LIMIT, 5);

        SearchResponse searchResponse = mock(SearchResponse.class);
        HitsMetadata hitsMetadata = mock(HitsMetadata.class);
        when(hitsMetadata.hits()).thenReturn(List.of());
        when(searchResponse.hits()).thenReturn(hitsMetadata);
        when(searchResponse.aggregations()).thenReturn(Map.of());
        when(esUtilService.popularCommunities(any(SearchRequest.class), any()))
            .thenReturn(searchResponse);

        ApiResponse response = service.getPopularCommunitiesByField(payload);

        assertEquals(HttpStatus.OK, response.getResponseCode());
    }

    @Test
    void getPopularCommunitiesByFieldReturnsHitsAndAggregationBuckets() {
        Map<String, Object> payload = new HashMap<>();
        payload.put(Constants.FIELD, "communityName");

        Map<String, Object> source = new HashMap<>();
        source.put(Constants.COMMUNITY_NAME, "Community One");
        Hit<Map> hit = Hit.of(h -> h.index("community-index").id("c1").source(source));
        HitsMetadata hitsMetadata = mock(HitsMetadata.class);
        when(hitsMetadata.hits()).thenReturn(List.of(hit));

        Aggregate aggregate = Aggregate.of(a -> a.sterms(s -> s.buckets(Buckets.of(bb -> bb.array(
            List.of(StringTermsBucket.of(bk -> bk.key("Community One").docCount(3))))))));

        SearchResponse searchResponse = mock(SearchResponse.class);
        when(searchResponse.hits()).thenReturn(hitsMetadata);
        when(searchResponse.aggregations()).thenReturn(Map.of("communityName_terms", aggregate));
        when(esUtilService.popularCommunities(any(SearchRequest.class), any()))
            .thenReturn(searchResponse);

        ApiResponse response = service.getPopularCommunitiesByField(payload);

        assertEquals(HttpStatus.OK, response.getResponseCode());
        List<?> facets = (List<?>) response.getResult().get(Constants.FACETS);
        assertEquals(1, facets.size());
        List<?> data = (List<?>) response.getResult().get(Constants.DATA);
        assertEquals(1, data.size());
    }

    @Test
    void getPopularCommunitiesByFieldSkipsBucketsWhenAggregateNotSterms() {
        Map<String, Object> payload = new HashMap<>();
        payload.put(Constants.FIELD, "communityName");

        HitsMetadata hitsMetadata = mock(HitsMetadata.class);
        when(hitsMetadata.hits()).thenReturn(List.of());

        Aggregate aggregate = Aggregate.of(a -> a.topHits(th -> th
            .hits(hm -> hm.total(t -> t.value(0).relation(TotalHitsRelation.Eq)).hits(List.of()))));

        SearchResponse searchResponse = mock(SearchResponse.class);
        when(searchResponse.hits()).thenReturn(hitsMetadata);
        when(searchResponse.aggregations()).thenReturn(Map.of("communityName_terms", aggregate));
        when(esUtilService.popularCommunities(any(SearchRequest.class), any()))
            .thenReturn(searchResponse);

        ApiResponse response = service.getPopularCommunitiesByField(payload);

        assertEquals(HttpStatus.OK, response.getResponseCode());
        assertEquals(false, response.getResult().containsKey(Constants.FACETS));
    }

    @Test
    void getPopularCommunitiesByFieldThrowsCustomExceptionOnUnexpectedError() {
        Map<String, Object> payload = new HashMap<>();
        payload.put(Constants.FIELD, "communityName");
        when(esUtilService.popularCommunities(any(SearchRequest.class), any()))
            .thenThrow(new RuntimeException("es down"));

        assertThrows(CustomException.class, () -> service.getPopularCommunitiesByField(payload));
    }
}
