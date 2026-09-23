package com.igot.cb.community.service.impl;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.igot.cb.authentication.util.AccessTokenValidator;
import com.igot.cb.community.entity.CommunityEntity;
import com.igot.cb.community.kafka.producer.Producer;
import com.igot.cb.community.repository.CommunityCategoryRepository;
import com.igot.cb.community.repository.CommunityEngagementRepository;
import com.igot.cb.community.service.NotificationService;
import com.igot.cb.community.service.UserService;
import com.igot.cb.pores.cache.CacheService;
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
import org.springframework.http.HttpStatus;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class CommunityManagementServiceImplJoinTest {

    @Mock private EsUtilService esUtilService;
    @Mock private CacheService cacheService;
    @Mock private PayloadValidation payloadValidation;
    @Mock private CommunityEngagementRepository communityEngagementRepository;
    @Mock private AccessTokenValidator accessTokenValidator;
    @Mock private CassandraOperation cassandraOperation;
    @Mock private RedisTemplate<String, SearchResult> redisTemplate;
    @Mock private CommunityCategoryRepository categoryRepository;
    @Mock private Producer producer;
    @Mock private UserService userService;
    @Mock private NotificationService notificationService;
    @Mock private FileProcessService fileProcessService;

    private CommunityManagementServiceImpl service;
    private final ObjectMapper objectMapper = new ObjectMapper();

    private static final String TOKEN = "auth-token";
    private static final String USER_ID = "user-1";
    private static final String COMMUNITY_ID = "community-1";

    @BeforeEach
    void setUp() {
        CbServerProperties cbServerProperties = new CbServerProperties();
        cbServerProperties.setJwtSecretKey("test-secret-key");
        cbServerProperties.setCommunityAdminJoinMaxUser(50);
        service = new CommunityManagementServiceImpl(esUtilService, cacheService, objectMapper,
            cbServerProperties, payloadValidation, communityEngagementRepository, accessTokenValidator,
            cassandraOperation, redisTemplate, categoryRepository, producer, userService,
            notificationService, fileProcessService);
        org.springframework.test.util.ReflectionTestUtils.setField(service, "userCountUpdateTopic", "user-count-topic");
        lenient().when(accessTokenValidator.verifyUserToken(TOKEN)).thenReturn(USER_ID);
    }

    private ObjectNode communityData(boolean isPrivate) {
        ObjectNode node = objectMapper.createObjectNode();
        node.put(Constants.COMMUNITY_NAME, "My Community");
        node.put(Constants.COMMUNITY_ID, COMMUNITY_ID);
        node.put(Constants.COUNT_OF_PEOPLE_JOINED, 5);
        if (isPrivate) {
            node.put(Constants.COMMUNITY_ACCESS_LEVEL, Constants.PRIVATE);
        }
        return node;
    }

    private CommunityEntity communityEntity(JsonNode data) {
        CommunityEntity entity = new CommunityEntity();
        entity.setCommunityId(COMMUNITY_ID);
        entity.setData(data);
        entity.setActive(true);
        entity.setUpdatedOn(new Timestamp(System.currentTimeMillis()));
        return entity;
    }

    private Map<String, Object> joinRequest() {
        Map<String, Object> request = new HashMap<>();
        request.put(Constants.COMMUNITY_ID, COMMUNITY_ID);
        return request;
    }

    // ---- joinCommunity ----

    @Test
    void joinCommunityReturnsBadRequestWhenUserIdBlank() {
        ApiResponse response = service.joinCommunity(joinRequest(), "bad-token");

        assertEquals(HttpStatus.BAD_REQUEST, response.getResponseCode());
    }

    @Test
    void joinCommunityReturnsBadRequestWhenPayloadInvalid() {
        Map<String, Object> request = new HashMap<>();
        request.put(Constants.COMMUNITY_ID, "   ");

        ApiResponse response = service.joinCommunity(request, TOKEN);

        assertEquals(HttpStatus.BAD_REQUEST, response.getResponseCode());
    }

    @Test
    void joinCommunityReturnsBadRequestWhenCommunityNotFound() {
        when(communityEngagementRepository.findByCommunityIdAndIsActive(COMMUNITY_ID, true))
            .thenReturn(Optional.empty());

        ApiResponse response = service.joinCommunity(joinRequest(), TOKEN);

        assertEquals(HttpStatus.BAD_REQUEST, response.getResponseCode());
        assertEquals(Constants.INVALID_COMMUNITY_ID, response.getParams().getErr());
    }

    @Test
    void joinCommunityReturnsBadRequestForPrivateCommunity() {
        when(communityEngagementRepository.findByCommunityIdAndIsActive(COMMUNITY_ID, true))
            .thenReturn(Optional.of(communityEntity(communityData(true))));

        ApiResponse response = service.joinCommunity(joinRequest(), TOKEN);

        assertEquals(HttpStatus.BAD_REQUEST, response.getResponseCode());
    }

    @Test
    void joinCommunityInsertsNewRecordWhenNotJoinedYet() {
        when(communityEngagementRepository.findByCommunityIdAndIsActive(COMMUNITY_ID, true))
            .thenReturn(Optional.of(communityEntity(communityData(false))));
        when(cassandraOperation.getRecordsByPropertiesWithoutFiltering(
            eq(Constants.KEYSPACE_SUNBIRD), eq(Constants.USER_COMMUNITY_TABLE), any(Map.class), eq(null), eq(1)))
            .thenReturn(List.of());

        ApiResponse response = service.joinCommunity(joinRequest(), TOKEN);

        assertEquals(HttpStatus.OK, response.getResponseCode());
    }

    @Test
    void joinCommunityReturnsErrorWhenAlreadyJoined() {
        when(communityEngagementRepository.findByCommunityIdAndIsActive(COMMUNITY_ID, true))
            .thenReturn(Optional.of(communityEntity(communityData(false))));
        Map<String, Object> existing = new HashMap<>();
        existing.put(Constants.STATUS, true);
        when(cassandraOperation.getRecordsByPropertiesWithoutFiltering(
            eq(Constants.KEYSPACE_SUNBIRD), eq(Constants.USER_COMMUNITY_TABLE), any(Map.class), eq(null), eq(1)))
            .thenReturn(List.of(existing));

        ApiResponse response = service.joinCommunity(joinRequest(), TOKEN);

        assertEquals(HttpStatus.BAD_REQUEST, response.getResponseCode());
        assertEquals(Constants.ALREADY_JOINED_COMMUNITY, response.getParams().getErr());
    }

    @Test
    void joinCommunityReactivatesWhenPreviouslyLeft() {
        when(communityEngagementRepository.findByCommunityIdAndIsActive(COMMUNITY_ID, true))
            .thenReturn(Optional.of(communityEntity(communityData(false))));
        Map<String, Object> existing = new HashMap<>();
        existing.put(Constants.STATUS, false);
        when(cassandraOperation.getRecordsByPropertiesWithoutFiltering(
            eq(Constants.KEYSPACE_SUNBIRD), eq(Constants.USER_COMMUNITY_TABLE), any(Map.class), eq(null), eq(1)))
            .thenReturn(List.of(existing));

        ApiResponse response = service.joinCommunity(joinRequest(), TOKEN);

        assertEquals(HttpStatus.OK, response.getResponseCode());
    }

    @Test
    void joinCommunityThrowsCustomExceptionOnUnexpectedError() {
        when(communityEngagementRepository.findByCommunityIdAndIsActive(COMMUNITY_ID, true))
            .thenThrow(new RuntimeException("db down"));

        Map<String, Object> request = joinRequest();
        assertThrows(CustomException.class, () -> service.joinCommunity(request, TOKEN));
    }

    // ---- unJoinCommunity ----

    @Test
    void unJoinReturnsBadRequestWhenUserIdBlank() {
        ApiResponse response = service.unJoinCommunity(joinRequest(), "bad-token");

        assertEquals(HttpStatus.BAD_REQUEST, response.getResponseCode());
    }

    @Test
    void unJoinReturnsBadRequestWhenCommunityNotFound() {
        when(communityEngagementRepository.findByCommunityIdAndIsActive(COMMUNITY_ID, true))
            .thenReturn(Optional.empty());

        ApiResponse response = service.unJoinCommunity(joinRequest(), TOKEN);

        assertEquals(HttpStatus.BAD_REQUEST, response.getResponseCode());
    }

    @Test
    void unJoinReturnsBadRequestWhenNotJoined() {
        when(communityEngagementRepository.findByCommunityIdAndIsActive(COMMUNITY_ID, true))
            .thenReturn(Optional.of(communityEntity(communityData(false))));
        when(cassandraOperation.getRecordsByPropertiesWithoutFiltering(
            eq(Constants.KEYSPACE_SUNBIRD), eq(Constants.USER_COMMUNITY_TABLE), any(Map.class), eq(null), eq(1)))
            .thenReturn(List.of());

        ApiResponse response = service.unJoinCommunity(joinRequest(), TOKEN);

        assertEquals(HttpStatus.BAD_REQUEST, response.getResponseCode());
        assertEquals(Constants.NOT_JOINED_ALREADY, response.getParams().getErr());
    }

    @Test
    void unJoinReturnsBadRequestWhenStatusAlreadyFalse() {
        when(communityEngagementRepository.findByCommunityIdAndIsActive(COMMUNITY_ID, true))
            .thenReturn(Optional.of(communityEntity(communityData(false))));
        Map<String, Object> existing = new HashMap<>();
        existing.put(Constants.STATUS, false);
        when(cassandraOperation.getRecordsByPropertiesWithoutFiltering(
            eq(Constants.KEYSPACE_SUNBIRD), eq(Constants.USER_COMMUNITY_TABLE), any(Map.class), eq(null), eq(1)))
            .thenReturn(List.of(existing));

        ApiResponse response = service.unJoinCommunity(joinRequest(), TOKEN);

        assertEquals(HttpStatus.BAD_REQUEST, response.getResponseCode());
        assertEquals(Constants.NOT_JOINED_ALREADY, response.getParams().getErr());
    }

    @Test
    void unJoinReturnsSuccessOnHappyPath() {
        when(communityEngagementRepository.findByCommunityIdAndIsActive(COMMUNITY_ID, true))
            .thenReturn(Optional.of(communityEntity(communityData(false))));
        Map<String, Object> existing = new HashMap<>();
        existing.put(Constants.STATUS, true);
        when(cassandraOperation.getRecordsByPropertiesWithoutFiltering(
            eq(Constants.KEYSPACE_SUNBIRD), eq(Constants.USER_COMMUNITY_TABLE), any(Map.class), eq(null), eq(1)))
            .thenReturn(List.of(existing));

        ApiResponse response = service.unJoinCommunity(joinRequest(), TOKEN);

        assertEquals(HttpStatus.OK, response.getResponseCode());
    }

    @Test
    void unJoinThrowsCustomExceptionOnUnexpectedError() {
        when(communityEngagementRepository.findByCommunityIdAndIsActive(COMMUNITY_ID, true))
            .thenThrow(new RuntimeException("db down"));

        Map<String, Object> request = joinRequest();
        assertThrows(CustomException.class, () -> service.unJoinCommunity(request, TOKEN));
    }

    // ---- communitiesJoinedByUser ----

    @Test
    void communitiesJoinedByUserReturnsBadRequestWhenUserIdBlank() {
        ApiResponse response = service.communitiesJoinedByUser("bad-token");

        assertEquals(HttpStatus.BAD_REQUEST, response.getResponseCode());
    }

    @Test
    void communitiesJoinedByUserSkipsRecordWhenStatusFalse() {
        Map<String, Object> record = new HashMap<>();
        record.put(Constants.STATUS, false);
        record.put(Constants.COMMUNITY_ID_LOWERCASE, COMMUNITY_ID);
        when(cassandraOperation.getRecordsByPropertiesWithoutFiltering(
            eq(Constants.KEYSPACE_SUNBIRD), eq(Constants.USER_COMMUNITY_TABLE), any(Map.class), anyList(), eq(null)))
            .thenReturn(List.of(record));

        ApiResponse response = service.communitiesJoinedByUser(TOKEN);

        assertEquals(HttpStatus.OK, response.getResponseCode());
    }

    @Test
    void communitiesJoinedByUserReturnsFromCache() {
        Map<String, Object> record = new HashMap<>();
        record.put(Constants.STATUS, true);
        record.put(Constants.COMMUNITY_ID_LOWERCASE, COMMUNITY_ID);
        when(cassandraOperation.getRecordsByPropertiesWithoutFiltering(
            eq(Constants.KEYSPACE_SUNBIRD), eq(Constants.USER_COMMUNITY_TABLE), any(Map.class), anyList(), eq(null)))
            .thenReturn(List.of(record));
        when(cacheService.getCache(COMMUNITY_ID)).thenReturn("{\"communityName\":\"Cached\"}");

        ApiResponse response = service.communitiesJoinedByUser(TOKEN);

        assertEquals(HttpStatus.OK, response.getResponseCode());
    }

    @Test
    void communitiesJoinedByUserFallsBackToPrimaryWhenCacheMiss() {
        Map<String, Object> record = new HashMap<>();
        record.put(Constants.STATUS, true);
        record.put(Constants.COMMUNITY_ID_LOWERCASE, COMMUNITY_ID);
        when(cassandraOperation.getRecordsByPropertiesWithoutFiltering(
            eq(Constants.KEYSPACE_SUNBIRD), eq(Constants.USER_COMMUNITY_TABLE), any(Map.class), anyList(), eq(null)))
            .thenReturn(List.of(record));
        when(cacheService.getCache(COMMUNITY_ID)).thenReturn(null);
        when(communityEngagementRepository.findByCommunityIdAndIsActive(COMMUNITY_ID, true))
            .thenReturn(Optional.of(communityEntity(communityData(false))));

        ApiResponse response = service.communitiesJoinedByUser(TOKEN);

        assertEquals(HttpStatus.OK, response.getResponseCode());
    }

    @Test
    void communitiesJoinedByUserThrowsCustomExceptionForInvalidCachedJson() {
        Map<String, Object> record = new HashMap<>();
        record.put(Constants.STATUS, true);
        record.put(Constants.COMMUNITY_ID_LOWERCASE, COMMUNITY_ID);
        when(cassandraOperation.getRecordsByPropertiesWithoutFiltering(
            eq(Constants.KEYSPACE_SUNBIRD), eq(Constants.USER_COMMUNITY_TABLE), any(Map.class), anyList(), eq(null)))
            .thenReturn(List.of(record));
        when(cacheService.getCache(COMMUNITY_ID)).thenReturn("not-valid-json");

        assertThrows(CustomException.class, () -> service.communitiesJoinedByUser(TOKEN));
    }

    @Test
    void communitiesJoinedByUserThrowsCustomExceptionOnUnexpectedError() {
        when(cassandraOperation.getRecordsByPropertiesWithoutFiltering(
            eq(Constants.KEYSPACE_SUNBIRD), eq(Constants.USER_COMMUNITY_TABLE), any(Map.class), anyList(), eq(null)))
            .thenThrow(new RuntimeException("db down"));

        assertThrows(CustomException.class, () -> service.communitiesJoinedByUser(TOKEN));
    }

    // ---- listOfUsersJoined ----

    private Map<String, Object> listUsersPayload() {
        Map<String, Object> payload = new HashMap<>();
        payload.put(Constants.COMMUNITY_ID, COMMUNITY_ID);
        payload.put(Constants.OFFSET, 0);
        payload.put(Constants.LIMIT, 10);
        return payload;
    }

    @Test
    void listOfUsersJoinedReturnsBadRequestWhenPayloadInvalid() {
        ApiResponse response = service.listOfUsersJoined(TOKEN, new HashMap<>());

        assertEquals(HttpStatus.BAD_REQUEST, response.getResponseCode());
    }

    @Test
    void listOfUsersJoinedReturnsBadRequestWhenUserIdBlank() {
        ApiResponse response = service.listOfUsersJoined("bad-token", listUsersPayload());

        assertEquals(HttpStatus.BAD_REQUEST, response.getResponseCode());
    }

    @Test
    void listOfUsersJoinedReturnsEmptyWhenNoUsersFound() {
        when(cacheService.getListSize(anyString())).thenReturn(0L);
        when(cassandraOperation.getRecordsByPropertiesWithoutFiltering(
            eq(Constants.KEYSPACE_SUNBIRD), eq(Constants.USER_COMMUNITY_LOOK_UP_TABLE), any(Map.class), anyList(), eq(null)))
            .thenReturn(List.of());

        ApiResponse response = service.listOfUsersJoined(TOKEN, listUsersPayload());

        assertEquals(HttpStatus.OK, response.getResponseCode());
        assertEquals(0L, response.getResult().get(Constants.USER_COUNT));
    }

    @Test
    void listOfUsersJoinedReturnsSuccessWithCacheHit() {
        when(cacheService.getListSize(anyString())).thenReturn(5L);
        when(cacheService.getPaginatedUsersFromHash(anyString(), eq(0), eq(10)))
            .thenReturn(List.of("user:u1"));
        when(cacheService.hget(anyList())).thenReturn(
            new ArrayList<>(List.of("{\"user_id\":\"u1\",\"firstName\":\"Alice\"}")));

        ApiResponse response = service.listOfUsersJoined(TOKEN, listUsersPayload());

        assertEquals(HttpStatus.OK, response.getResponseCode());
        assertEquals(5L, response.getResult().get(Constants.USER_COUNT));
    }

    @Test
    void listOfUsersJoinedFallsBackToCassandraForMissingUsers() {
        when(cacheService.getListSize(anyString())).thenReturn(5L);
        when(cacheService.getPaginatedUsersFromHash(anyString(), eq(0), eq(10)))
            .thenReturn(List.of("user:u1"));
        List<Object> hgetResult = new ArrayList<>();
        hgetResult.add(null);
        when(cacheService.hget(anyList())).thenReturn(hgetResult);
        when(userService.fetchUserFromprimary(anyList())).thenReturn(List.of(Map.of("user_id", "u1")));

        ApiResponse response = service.listOfUsersJoined(TOKEN, listUsersPayload());

        assertEquals(HttpStatus.OK, response.getResponseCode());
    }

    @Test
    void listOfUsersJoinedReturnsEmptyWhenStartIndexExceedsTotalCount() {
        Map<String, Object> payload = new HashMap<>();
        payload.put(Constants.COMMUNITY_ID, COMMUNITY_ID);
        payload.put(Constants.OFFSET, 1);
        payload.put(Constants.LIMIT, 10);
        when(cacheService.getListSize(anyString())).thenReturn(5L);

        ApiResponse response = service.listOfUsersJoined(TOKEN, payload);

        assertEquals(HttpStatus.OK, response.getResponseCode());
        assertEquals(0L, response.getResult().get(Constants.USER_COUNT));
    }

    @Test
    void listOfUsersJoinedRefetchesListSizeWhenInitiallyZeroButPrimaryHasData() {
        when(cacheService.getListSize(anyString())).thenReturn(0L, 5L);
        Map<String, Object> lookupRecord = new HashMap<>();
        lookupRecord.put(Constants.STATUS, true);
        lookupRecord.put(Constants.USER_ID_LOWER_CASE, "u1");
        when(cassandraOperation.getRecordsByPropertiesWithoutFiltering(
            eq(Constants.KEYSPACE_SUNBIRD), eq(Constants.USER_COMMUNITY_LOOK_UP_TABLE), any(Map.class), anyList(), eq(null)))
            .thenReturn(List.of(lookupRecord));
        when(cacheService.getPaginatedUsersFromHash(anyString(), eq(0), eq(10)))
            .thenReturn(List.of("user:u1"));
        when(cacheService.hget(anyList())).thenReturn(
            new ArrayList<>(List.of("{\"user_id\":\"u1\",\"firstName\":\"Alice\"}")));

        ApiResponse response = service.listOfUsersJoined(TOKEN, listUsersPayload());

        assertEquals(HttpStatus.OK, response.getResponseCode());
        assertEquals(5L, response.getResult().get(Constants.USER_COUNT));
    }

    @Test
    void listOfUsersJoinedFallsBackToPrimaryWhenPaginatedHashEmpty() {
        when(cacheService.getListSize(anyString())).thenReturn(5L);
        Map<String, Object> lookupRecord = new HashMap<>();
        lookupRecord.put(Constants.STATUS, true);
        lookupRecord.put(Constants.USER_ID_LOWER_CASE, "u1");
        when(cassandraOperation.getRecordsByPropertiesWithoutFiltering(
            eq(Constants.KEYSPACE_SUNBIRD), eq(Constants.USER_COMMUNITY_LOOK_UP_TABLE), any(Map.class), anyList(), eq(null)))
            .thenReturn(List.of(lookupRecord));
        when(cacheService.getPaginatedUsersFromHash(anyString(), eq(0), eq(10)))
            .thenReturn(List.of(), List.of("user:u1"));
        when(cacheService.hget(anyList())).thenReturn(
            new ArrayList<>(List.of("{\"user_id\":\"u1\",\"firstName\":\"Alice\"}")));

        ApiResponse response = service.listOfUsersJoined(TOKEN, listUsersPayload());

        assertEquals(HttpStatus.OK, response.getResponseCode());
    }

    @Test
    void listOfUsersJoinedFetchDataFromPrimaryReturnsEmptyWhenNoStatusTrueRecords() {
        when(cacheService.getListSize(anyString())).thenReturn(0L, 0L);
        Map<String, Object> lookupRecord = new HashMap<>();
        lookupRecord.put(Constants.STATUS, false);
        lookupRecord.put(Constants.USER_ID_LOWER_CASE, "u1");
        when(cassandraOperation.getRecordsByPropertiesWithoutFiltering(
            eq(Constants.KEYSPACE_SUNBIRD), eq(Constants.USER_COMMUNITY_LOOK_UP_TABLE), any(Map.class), anyList(), eq(null)))
            .thenReturn(List.of(lookupRecord));

        ApiResponse response = service.listOfUsersJoined(TOKEN, listUsersPayload());

        assertEquals(HttpStatus.OK, response.getResponseCode());
        assertEquals(0L, response.getResult().get(Constants.USER_COUNT));
    }

    @Test
    void listOfUsersJoinedCleansLiteralNullDesignationString() {
        when(cacheService.getListSize(anyString())).thenReturn(5L);
        when(cacheService.getPaginatedUsersFromHash(anyString(), eq(0), eq(10)))
            .thenReturn(List.of("user:u1"));
        when(cacheService.hget(anyList())).thenReturn(new ArrayList<>(
            List.of("{\"user_id\":\"u1\",\"firstName\":\"Alice\",\"designation\":\"null\"}")));

        ApiResponse response = service.listOfUsersJoined(TOKEN, listUsersPayload());

        assertEquals(HttpStatus.OK, response.getResponseCode());
    }

    @Test
    void listOfUsersJoinedHandlesInvalidJsonFromCache() {
        when(cacheService.getListSize(anyString())).thenReturn(5L);
        when(cacheService.getPaginatedUsersFromHash(anyString(), eq(0), eq(10)))
            .thenReturn(List.of("user:u1"));
        when(cacheService.hget(anyList())).thenReturn(new ArrayList<>(List.of("not-valid-json")));

        Map<String, Object> payload = listUsersPayload();
        assertThrows(CustomException.class, () -> service.listOfUsersJoined(TOKEN, payload));
    }

    @Test
    void listOfUsersJoinedReturnsBadRequestWhenCommunityIdBlank() {
        Map<String, Object> payload = new HashMap<>();
        payload.put(Constants.COMMUNITY_ID, "   ");
        payload.put(Constants.OFFSET, 0);
        payload.put(Constants.LIMIT, 10);

        ApiResponse response = service.listOfUsersJoined(TOKEN, payload);

        assertEquals(HttpStatus.BAD_REQUEST, response.getResponseCode());
    }

    @Test
    void listOfUsersJoinedReturnsBadRequestWhenOffsetWrongType() {
        Map<String, Object> payload = new HashMap<>();
        payload.put(Constants.COMMUNITY_ID, COMMUNITY_ID);
        payload.put(Constants.OFFSET, "zero");
        payload.put(Constants.LIMIT, 10);

        ApiResponse response = service.listOfUsersJoined(TOKEN, payload);

        assertEquals(HttpStatus.BAD_REQUEST, response.getResponseCode());
    }

    @Test
    void listOfUsersJoinedReturnsBadRequestWhenLimitNull() {
        Map<String, Object> payload = new HashMap<>();
        payload.put(Constants.COMMUNITY_ID, COMMUNITY_ID);
        payload.put(Constants.OFFSET, 0);
        payload.put(Constants.LIMIT, null);

        ApiResponse response = service.listOfUsersJoined(TOKEN, payload);

        assertEquals(HttpStatus.BAD_REQUEST, response.getResponseCode());
    }

    @Test
    void listOfUsersJoinedThrowsCustomExceptionOnUnexpectedError() {
        when(cacheService.getListSize(anyString())).thenThrow(new RuntimeException("redis down"));

        Map<String, Object> payload = listUsersPayload();
        assertThrows(CustomException.class, () -> service.listOfUsersJoined(TOKEN, payload));
    }

    // ---- listAllCommunitiesJoinedByUser ----

    @Test
    void listAllCommunitiesJoinedByUserReturnsBadRequestWhenUserIdBlank() {
        ApiResponse response = service.listAllCommunitiesJoinedByUser("bad-token");

        assertEquals(HttpStatus.BAD_REQUEST, response.getResponseCode());
    }

    @Test
    void listAllCommunitiesJoinedByUserSkipsInactiveAndIncludesActive() {
        Map<String, Object> inactive = new HashMap<>();
        inactive.put(Constants.STATUS, false);
        inactive.put(Constants.COMMUNITY_ID_LOWERCASE, "c-2");
        Map<String, Object> active = new HashMap<>();
        active.put(Constants.STATUS, true);
        active.put(Constants.COMMUNITY_ID_LOWERCASE, COMMUNITY_ID);
        when(cassandraOperation.getRecordsByPropertiesWithoutFiltering(
            eq(Constants.KEYSPACE_SUNBIRD), eq(Constants.USER_COMMUNITY_TABLE), any(Map.class), anyList(), eq(null)))
            .thenReturn(List.of(inactive, active));
        when(cacheService.getCache(COMMUNITY_ID)).thenReturn(null);
        when(communityEngagementRepository.findByCommunityIdAndIsActive(COMMUNITY_ID, true))
            .thenReturn(Optional.of(communityEntity(communityData(false))));

        ApiResponse response = service.listAllCommunitiesJoinedByUser(TOKEN);

        assertEquals(HttpStatus.OK, response.getResponseCode());
    }

    @Test
    void listAllCommunitiesJoinedByUserReadsFromCacheWhenPresent() {
        Map<String, Object> active = new HashMap<>();
        active.put(Constants.STATUS, true);
        active.put(Constants.COMMUNITY_ID_LOWERCASE, COMMUNITY_ID);
        when(cassandraOperation.getRecordsByPropertiesWithoutFiltering(
            eq(Constants.KEYSPACE_SUNBIRD), eq(Constants.USER_COMMUNITY_TABLE), any(Map.class), anyList(), eq(null)))
            .thenReturn(List.of(active));
        when(cacheService.getCache(COMMUNITY_ID)).thenReturn(
            "{\"communityName\":\"My Community\",\"communityId\":\"" + COMMUNITY_ID + "\"}");

        ApiResponse response = service.listAllCommunitiesJoinedByUser(TOKEN);

        assertEquals(HttpStatus.OK, response.getResponseCode());
    }

    @Test
    void listAllCommunitiesJoinedByUserSkipsWhenCommunityMapMissingRequiredFields() {
        Map<String, Object> active = new HashMap<>();
        active.put(Constants.STATUS, true);
        active.put(Constants.COMMUNITY_ID_LOWERCASE, COMMUNITY_ID);
        when(cassandraOperation.getRecordsByPropertiesWithoutFiltering(
            eq(Constants.KEYSPACE_SUNBIRD), eq(Constants.USER_COMMUNITY_TABLE), any(Map.class), anyList(), eq(null)))
            .thenReturn(List.of(active));
        when(cacheService.getCache(COMMUNITY_ID)).thenReturn(null);
        when(communityEngagementRepository.findByCommunityIdAndIsActive(COMMUNITY_ID, true))
            .thenReturn(Optional.empty());

        ApiResponse response = service.listAllCommunitiesJoinedByUser(TOKEN);

        assertEquals(HttpStatus.OK, response.getResponseCode());
    }

    @Test
    void listAllCommunitiesJoinedByUserThrowsCustomExceptionOnUnexpectedError() {
        when(cassandraOperation.getRecordsByPropertiesWithoutFiltering(
            eq(Constants.KEYSPACE_SUNBIRD), eq(Constants.USER_COMMUNITY_TABLE), any(Map.class), anyList(), eq(null)))
            .thenThrow(new RuntimeException("db down"));

        assertThrows(CustomException.class, () -> service.listAllCommunitiesJoinedByUser(TOKEN));
    }

    // ---- adminJoinCommunity / adminUnjoinCommunity ----

    private Map<String, Object> adminRequest(List<String> userIds) {
        Map<String, Object> request = new HashMap<>();
        request.put(Constants.COMMUNITY_ID, COMMUNITY_ID);
        request.put(Constants.USER_IDS, userIds);
        return request;
    }

    @Test
    void adminJoinCommunityReturnsBadRequestWhenUserIdBlank() {
        ApiResponse response = service.adminJoinCommunity(adminRequest(List.of("u1")), "bad-token");

        assertEquals(HttpStatus.BAD_REQUEST, response.getResponseCode());
    }

    @Test
    void adminJoinCommunityReturnsBadRequestWhenUserIdsInvalid() {
        Map<String, Object> request = new HashMap<>();
        request.put(Constants.COMMUNITY_ID, COMMUNITY_ID);
        request.put(Constants.USER_IDS, "not-a-list");

        ApiResponse response = service.adminJoinCommunity(request, TOKEN);

        assertEquals(HttpStatus.BAD_REQUEST, response.getResponseCode());
    }

    @Test
    void adminJoinCommunityReturnsBadRequestWhenTooManyUserIds() {
        Map<String, Object> request = new HashMap<>();
        request.put(Constants.COMMUNITY_ID, COMMUNITY_ID);
        request.put(Constants.USER_IDS, List.of("u1", "u2", "u3"));
        CbServerProperties limitedProps = new CbServerProperties();
        limitedProps.setCommunityAdminJoinMaxUser(2);
        CommunityManagementServiceImpl limitedService = new CommunityManagementServiceImpl(esUtilService, cacheService,
            objectMapper, limitedProps, payloadValidation, communityEngagementRepository, accessTokenValidator,
            cassandraOperation, redisTemplate, categoryRepository, producer, userService, notificationService,
            fileProcessService);
        when(accessTokenValidator.verifyUserToken(TOKEN)).thenReturn(USER_ID);

        ApiResponse response = limitedService.adminJoinCommunity(request, TOKEN);

        assertEquals(HttpStatus.BAD_REQUEST, response.getResponseCode());
    }

    @Test
    void adminJoinCommunityReturnsBadRequestWhenCommunityMissing() {
        when(communityEngagementRepository.findByCommunityIdAndIsActive(COMMUNITY_ID, true))
            .thenReturn(Optional.empty());

        ApiResponse response = service.adminJoinCommunity(adminRequest(List.of("u1")), TOKEN);

        assertEquals(HttpStatus.BAD_REQUEST, response.getResponseCode());
    }

    @Test
    void adminJoinCommunityJoinsAlreadyJoinedAndFailedUsers() {
        when(communityEngagementRepository.findByCommunityIdAndIsActive(COMMUNITY_ID, true))
            .thenReturn(Optional.of(communityEntity(communityData(false))));

        when(cassandraOperation.getRecordsByPropertiesWithoutFiltering(
            eq(Constants.KEYSPACE_SUNBIRD), eq(Constants.USER_COMMUNITY_TABLE), any(Map.class), eq(null), eq(1)))
            .thenReturn(List.of())
            .thenThrow(new RuntimeException("cassandra error"));

        ApiResponse response = service.adminJoinCommunity(adminRequest(List.of("u1", "u2")), TOKEN);

        assertEquals(HttpStatus.OK, response.getResponseCode());
        Map<String, Object> result = response.getResult();
        assertTrue(((List<?>) result.get(Constants.JOINED_USERS)).contains("u1"));
        assertTrue(((List<?>) result.get(Constants.FAILED_USERS)).contains("u2"));
    }

    @Test
    void adminJoinCommunityMarksAlreadyJoinedWhenStatusTrue() {
        when(communityEngagementRepository.findByCommunityIdAndIsActive(COMMUNITY_ID, true))
            .thenReturn(Optional.of(communityEntity(communityData(false))));
        Map<String, Object> existing = new HashMap<>();
        existing.put(Constants.STATUS, true);
        when(cassandraOperation.getRecordsByPropertiesWithoutFiltering(
            eq(Constants.KEYSPACE_SUNBIRD), eq(Constants.USER_COMMUNITY_TABLE), any(Map.class), eq(null), eq(1)))
            .thenReturn(List.of(existing));

        ApiResponse response = service.adminJoinCommunity(adminRequest(List.of("u1")), TOKEN);

        assertEquals(HttpStatus.OK, response.getResponseCode());
        assertTrue(((List<?>) response.getResult().get(Constants.ALREADY_JOINED_USERS)).contains("u1"));
    }

    @Test
    void adminJoinCommunityReactivatesWhenStatusFalse() {
        when(communityEngagementRepository.findByCommunityIdAndIsActive(COMMUNITY_ID, true))
            .thenReturn(Optional.of(communityEntity(communityData(false))));
        Map<String, Object> existing = new HashMap<>();
        existing.put(Constants.STATUS, false);
        when(cassandraOperation.getRecordsByPropertiesWithoutFiltering(
            eq(Constants.KEYSPACE_SUNBIRD), eq(Constants.USER_COMMUNITY_TABLE), any(Map.class), eq(null), eq(1)))
            .thenReturn(List.of(existing));

        ApiResponse response = service.adminJoinCommunity(adminRequest(List.of("u1")), TOKEN);

        assertEquals(HttpStatus.OK, response.getResponseCode());
        assertTrue(((List<?>) response.getResult().get(Constants.JOINED_USERS)).contains("u1"));
    }

    @Test
    void adminJoinCommunityThrowsCustomExceptionOnUnexpectedError() {
        when(communityEngagementRepository.findByCommunityIdAndIsActive(COMMUNITY_ID, true))
            .thenThrow(new RuntimeException("db down"));

        Map<String, Object> request = adminRequest(List.of("u1"));
        assertThrows(CustomException.class, () -> service.adminJoinCommunity(request, TOKEN));
    }

    @Test
    void adminUnjoinCommunityReturnsBadRequestWhenUserIdBlank() {
        ApiResponse response = service.adminUnjoinCommunity(adminRequest(List.of("u1")), "bad-token");

        assertEquals(HttpStatus.BAD_REQUEST, response.getResponseCode());
    }

    @Test
    void adminUnjoinCommunityUnjoinsAndReportsNotJoined() {
        when(communityEngagementRepository.findByCommunityIdAndIsActive(COMMUNITY_ID, true))
            .thenReturn(Optional.of(communityEntity(communityData(false))));

        when(cassandraOperation.getRecordsByPropertiesWithoutFiltering(
            eq(Constants.KEYSPACE_SUNBIRD), eq(Constants.USER_COMMUNITY_TABLE), any(Map.class), eq(null), eq(1)))
            .thenReturn(List.of());

        ApiResponse response = service.adminUnjoinCommunity(adminRequest(List.of("u1")), TOKEN);

        assertEquals(HttpStatus.OK, response.getResponseCode());
        Map<String, Object> result = response.getResult();
        assertTrue(((List<?>) result.get(Constants.NOT_JOINED_USERS)).contains("u1"));
    }

    @Test
    void adminUnjoinCommunityReturnsNotJoinedWhenStatusAlreadyFalse() {
        when(communityEngagementRepository.findByCommunityIdAndIsActive(COMMUNITY_ID, true))
            .thenReturn(Optional.of(communityEntity(communityData(false))));
        Map<String, Object> existing = new HashMap<>();
        existing.put(Constants.STATUS, false);
        when(cassandraOperation.getRecordsByPropertiesWithoutFiltering(
            eq(Constants.KEYSPACE_SUNBIRD), eq(Constants.USER_COMMUNITY_TABLE), any(Map.class), eq(null), eq(1)))
            .thenReturn(List.of(existing));

        ApiResponse response = service.adminUnjoinCommunity(adminRequest(List.of("u1")), TOKEN);

        assertEquals(HttpStatus.OK, response.getResponseCode());
        assertTrue(((List<?>) response.getResult().get(Constants.NOT_JOINED_USERS)).contains("u1"));
    }

    @Test
    void adminUnjoinCommunitySucceedsWhenStatusTrue() {
        when(communityEngagementRepository.findByCommunityIdAndIsActive(COMMUNITY_ID, true))
            .thenReturn(Optional.of(communityEntity(communityData(false))));
        Map<String, Object> existing = new HashMap<>();
        existing.put(Constants.STATUS, true);
        when(cassandraOperation.getRecordsByPropertiesWithoutFiltering(
            eq(Constants.KEYSPACE_SUNBIRD), eq(Constants.USER_COMMUNITY_TABLE), any(Map.class), eq(null), eq(1)))
            .thenReturn(List.of(existing));

        ApiResponse response = service.adminUnjoinCommunity(adminRequest(List.of("u1")), TOKEN);

        assertEquals(HttpStatus.OK, response.getResponseCode());
        assertTrue(((List<?>) response.getResult().get(Constants.UNJOINED_USERS)).contains("u1"));
        org.mockito.Mockito.verify(cacheService, org.mockito.Mockito.atLeastOnce()).deleteCache(anyString());
    }

    @Test
    void adminUnjoinCommunityAddsFailedUserOnException() {
        when(communityEngagementRepository.findByCommunityIdAndIsActive(COMMUNITY_ID, true))
            .thenReturn(Optional.of(communityEntity(communityData(false))));
        when(cassandraOperation.getRecordsByPropertiesWithoutFiltering(
            eq(Constants.KEYSPACE_SUNBIRD), eq(Constants.USER_COMMUNITY_TABLE), any(Map.class), eq(null), eq(1)))
            .thenThrow(new RuntimeException("cassandra error"));

        ApiResponse response = service.adminUnjoinCommunity(adminRequest(List.of("u1")), TOKEN);

        assertEquals(HttpStatus.OK, response.getResponseCode());
        assertTrue(((List<?>) response.getResult().get(Constants.FAILED_USERS)).contains("u1"));
    }

    @Test
    void adminUnjoinCommunityThrowsCustomExceptionOnUnexpectedError() {
        when(communityEngagementRepository.findByCommunityIdAndIsActive(COMMUNITY_ID, true))
            .thenThrow(new RuntimeException("db down"));

        Map<String, Object> request = adminRequest(List.of("u1"));
        assertThrows(CustomException.class, () -> service.adminUnjoinCommunity(request, TOKEN));
    }
}
