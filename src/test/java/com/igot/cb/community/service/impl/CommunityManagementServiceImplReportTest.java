package com.igot.cb.community.service.impl;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
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

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class CommunityManagementServiceImplReportTest {

    @Mock
    private EsUtilService esUtilService;
    @Mock
    private CacheService cacheService;
    @Mock
    private PayloadValidation payloadValidation;
    @Mock
    private CommunityEngagementRepository communityEngagementRepository;
    @Mock
    private AccessTokenValidator accessTokenValidator;
    @Mock
    private CassandraOperation cassandraOperation;
    @Mock
    private RedisTemplate<String, SearchResult> redisTemplate;
    @Mock
    private CommunityCategoryRepository categoryRepository;
    @Mock
    private Producer producer;
    @Mock
    private UserService userService;
    @Mock
    private NotificationService notificationService;
    @Mock
    private FileProcessService fileProcessService;

    private CommunityManagementServiceImpl service;
    private ObjectMapper objectMapper;

    @BeforeEach
    void setUp() {
        objectMapper = new ObjectMapper();
        CbServerProperties cbServerProperties = new CbServerProperties();
        cbServerProperties.setReporCommunityUserLimit(3);
        service = new CommunityManagementServiceImpl(esUtilService, cacheService, objectMapper,
            cbServerProperties, payloadValidation, communityEngagementRepository, accessTokenValidator,
            cassandraOperation, redisTemplate, categoryRepository, producer, userService,
            notificationService, fileProcessService);
    }

    private Map<String, Object> reportPayload(String communityId, List<String> reasons, String otherReason) {
        Map<String, Object> payload = new HashMap<>();
        payload.put(Constants.COMMUNITY_ID, communityId);
        if (reasons != null) {
            payload.put(Constants.REPORTED_REASON, reasons);
        }
        if (otherReason != null) {
            payload.put(Constants.OTHER_REASON, otherReason);
        }
        return payload;
    }

    private CommunityEntity activeCommunity(String communityId, String status) throws Exception {
        JsonNode data = objectMapper.readTree(
            "{\"communityId\":\"" + communityId + "\",\"status\":\"" + status + "\"}");
        CommunityEntity entity = new CommunityEntity();
        entity.setCommunityId(communityId);
        entity.setData(data);
        entity.setActive(true);
        return entity;
    }

    @Test
    void reportReturnsBadRequestWhenCommunityIdBlank() {
        Map<String, Object> payload = reportPayload("   ", null, null);

        ApiResponse response = service.report("token", payload);

        assertEquals(HttpStatus.BAD_REQUEST, response.getResponseCode());
    }

    @Test
    void reportReturnsBadRequestWhenReportedReasonEmpty() {
        Map<String, Object> payload = reportPayload("c1", Collections.emptyList(), null);

        ApiResponse response = service.report("token", payload);

        assertEquals(HttpStatus.BAD_REQUEST, response.getResponseCode());
    }

    @Test
    void reportReturnsBadRequestWhenReportedReasonNotAList() {
        Map<String, Object> payload = new HashMap<>();
        payload.put(Constants.COMMUNITY_ID, "c1");
        payload.put(Constants.REPORTED_REASON, "not-a-list");

        ApiResponse response = service.report("token", payload);

        assertEquals(HttpStatus.BAD_REQUEST, response.getResponseCode());
    }

    @Test
    void reportReturnsBadRequestWhenOthersReasonSelectedWithoutOtherReasonText() {
        Map<String, Object> payload = reportPayload("c1", List.of(Constants.OTHERS), null);

        ApiResponse response = service.report("token", payload);

        assertEquals(HttpStatus.BAD_REQUEST, response.getResponseCode());
    }

    @Test
    void reportReturnsUnauthorizedWhenTokenInvalid() {
        Map<String, Object> payload = reportPayload("c1", List.of("spam"), null);
        when(accessTokenValidator.verifyUserToken("token")).thenReturn(null);

        ApiResponse response = service.report("token", payload);

        assertEquals(HttpStatus.UNAUTHORIZED, response.getResponseCode());
    }

    @Test
    void reportReturnsNotFoundWhenCommunityMissing() {
        Map<String, Object> payload = reportPayload("c1", List.of("spam"), null);
        when(accessTokenValidator.verifyUserToken("token")).thenReturn("user1");
        when(communityEngagementRepository.findById("c1")).thenReturn(Optional.empty());

        ApiResponse response = service.report("token", payload);

        assertEquals(HttpStatus.NOT_FOUND, response.getResponseCode());
    }

    @Test
    void reportReturnsConflictWhenCommunityInactive() throws Exception {
        Map<String, Object> payload = reportPayload("c1", List.of("spam"), null);
        when(accessTokenValidator.verifyUserToken("token")).thenReturn("user1");
        CommunityEntity entity = activeCommunity("c1", Constants.ACTIVE);
        entity.setActive(false);
        when(communityEngagementRepository.findById("c1")).thenReturn(Optional.of(entity));

        ApiResponse response = service.report("token", payload);

        assertEquals(HttpStatus.CONFLICT, response.getResponseCode());
    }

    @Test
    void reportReturnsConflictWhenCommunityAlreadySuspended() throws Exception {
        Map<String, Object> payload = reportPayload("c1", List.of("spam"), null);
        when(accessTokenValidator.verifyUserToken("token")).thenReturn("user1");
        CommunityEntity entity = activeCommunity("c1", Constants.SUSPENDED);
        when(communityEngagementRepository.findById("c1")).thenReturn(Optional.of(entity));

        ApiResponse response = service.report("token", payload);

        assertEquals(HttpStatus.CONFLICT, response.getResponseCode());
    }

    @Test
    void reportReturnsConflictWhenUserAlreadyReported() throws Exception {
        Map<String, Object> payload = reportPayload("c1", List.of("spam"), null);
        when(accessTokenValidator.verifyUserToken("token")).thenReturn("user1");
        CommunityEntity entity = activeCommunity("c1", Constants.ACTIVE);
        when(communityEngagementRepository.findById("c1")).thenReturn(Optional.of(entity));
        when(cassandraOperation.getRecordsByPropertiesWithoutFiltering(
            eq(Constants.KEYSPACE_SUNBIRD), eq(Constants.USER_REPORTED_COMMUNITY), any(Map.class), any(), any()))
            .thenReturn(List.of(new HashMap<>()));

        ApiResponse response = service.report("token", payload);

        assertEquals(HttpStatus.CONFLICT, response.getResponseCode());
    }

    @Test
    void reportSucceedsWithReasonsAndOthersAndSetsStatusReported() throws Exception {
        Map<String, Object> payload = reportPayload("c1", List.of("spam", Constants.OTHERS), "custom reason text");
        when(accessTokenValidator.verifyUserToken("token")).thenReturn("user1");
        CommunityEntity entity = activeCommunity("c1", Constants.ACTIVE);
        when(communityEngagementRepository.findById("c1")).thenReturn(Optional.of(entity));
        when(cassandraOperation.getRecordsByPropertiesWithoutFiltering(
            eq(Constants.KEYSPACE_SUNBIRD), eq(Constants.USER_REPORTED_COMMUNITY), any(Map.class), any(), any()))
            .thenReturn(Collections.emptyList());
        when(cassandraOperation.getRecordsByPropertiesWithoutFiltering(
            eq(Constants.KEYSPACE_SUNBIRD), eq(Constants.COMMUNITY_REPORTED_BY_USER), any(Map.class), any(), any()))
            .thenReturn(Collections.emptyList());

        ApiResponse response = service.report("token", payload);

        assertEquals(HttpStatus.OK, response.getResponseCode());
        Map<String, Object> result = response.getResult();
        assertEquals(Constants.REPORTED, result.get(Constants.STATUS));
        assertEquals("c1", result.get(Constants.COMMUNITY_ID));
        verify(cassandraOperation, times(2)).insertRecord(eq(Constants.KEYSPACE_SUNBIRD), anyString(), any(Map.class));
    }

    @Test
    void reportSuspendsCommunityWhenReportCountReachesLimit() throws Exception {
        Map<String, Object> payload = reportPayload("c1", null, null);
        when(accessTokenValidator.verifyUserToken("token")).thenReturn("user1");
        CommunityEntity entity = activeCommunity("c1", Constants.ACTIVE);
        when(communityEngagementRepository.findById("c1")).thenReturn(Optional.of(entity));
        when(cassandraOperation.getRecordsByPropertiesWithoutFiltering(
            eq(Constants.KEYSPACE_SUNBIRD), eq(Constants.USER_REPORTED_COMMUNITY), any(Map.class), any(), any()))
            .thenReturn(Collections.emptyList());
        List<Map<String, Object>> threeReports = List.of(new HashMap<>(), new HashMap<>(), new HashMap<>());
        when(cassandraOperation.getRecordsByPropertiesWithoutFiltering(
            eq(Constants.KEYSPACE_SUNBIRD), eq(Constants.COMMUNITY_REPORTED_BY_USER), any(Map.class), any(), any()))
            .thenReturn(threeReports);

        ApiResponse response = service.report("token", payload);

        assertEquals(HttpStatus.OK, response.getResponseCode());
        assertEquals(Constants.SUSPENDED, response.getResult().get(Constants.STATUS));
    }

    @Test
    void reportAppendsToExistingReportedByArray() throws Exception {
        Map<String, Object> payload = reportPayload("c1", null, null);
        when(accessTokenValidator.verifyUserToken("token")).thenReturn("user1");
        JsonNode data = objectMapper.readTree(
            "{\"communityId\":\"c1\",\"status\":\"active\",\"reportedBy\":[\"user0\"]}");
        CommunityEntity entity = new CommunityEntity();
        entity.setCommunityId("c1");
        entity.setData(data);
        entity.setActive(true);
        when(communityEngagementRepository.findById("c1")).thenReturn(Optional.of(entity));
        when(cassandraOperation.getRecordsByPropertiesWithoutFiltering(
            eq(Constants.KEYSPACE_SUNBIRD), eq(Constants.USER_REPORTED_COMMUNITY), any(Map.class), any(), any()))
            .thenReturn(Collections.emptyList());
        when(cassandraOperation.getRecordsByPropertiesWithoutFiltering(
            eq(Constants.KEYSPACE_SUNBIRD), eq(Constants.COMMUNITY_REPORTED_BY_USER), any(Map.class), any(), any()))
            .thenReturn(Collections.emptyList());

        ApiResponse response = service.report("token", payload);

        assertEquals(HttpStatus.OK, response.getResponseCode());
    }

    @Test
    void reportAppendsToNonArrayExistingReportedByValue() throws Exception {
        Map<String, Object> payload = reportPayload("c1", null, null);
        when(accessTokenValidator.verifyUserToken("token")).thenReturn("user1");
        JsonNode data = objectMapper.readTree(
            "{\"communityId\":\"c1\",\"status\":\"active\",\"reportedBy\":\"user0\"}");
        CommunityEntity entity = new CommunityEntity();
        entity.setCommunityId("c1");
        entity.setData(data);
        entity.setActive(true);
        when(communityEngagementRepository.findById("c1")).thenReturn(Optional.of(entity));
        when(cassandraOperation.getRecordsByPropertiesWithoutFiltering(
            eq(Constants.KEYSPACE_SUNBIRD), eq(Constants.USER_REPORTED_COMMUNITY), any(Map.class), any(), any()))
            .thenReturn(Collections.emptyList());
        when(cassandraOperation.getRecordsByPropertiesWithoutFiltering(
            eq(Constants.KEYSPACE_SUNBIRD), eq(Constants.COMMUNITY_REPORTED_BY_USER), any(Map.class), any(), any()))
            .thenReturn(Collections.emptyList());

        ApiResponse response = service.report("token", payload);

        assertEquals(HttpStatus.OK, response.getResponseCode());
    }

    @Test
    void reportReturnsInternalServerErrorOnUnexpectedException() {
        Map<String, Object> payload = reportPayload("c1", List.of("spam"), null);
        when(accessTokenValidator.verifyUserToken("token")).thenReturn("user1");
        when(communityEngagementRepository.findById("c1")).thenThrow(new RuntimeException("db down"));

        ApiResponse response = service.report("token", payload);

        assertEquals(HttpStatus.INTERNAL_SERVER_ERROR, response.getResponseCode());
        assertEquals(Constants.COMMUNITY_REPORT_FAILED, response.getParams().getErr());
    }
}
