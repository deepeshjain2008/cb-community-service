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
import org.springframework.mock.web.MockMultipartFile;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class CommunityManagementServiceImplPublishTest {

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

    @BeforeEach
    void setUp() {
        CbServerProperties cbServerProperties = new CbServerProperties();
        cbServerProperties.setJwtSecretKey("test-secret-key");
        service = new CommunityManagementServiceImpl(esUtilService, cacheService, objectMapper,
            cbServerProperties, payloadValidation, communityEngagementRepository, accessTokenValidator,
            cassandraOperation, redisTemplate, categoryRepository, producer, userService,
            notificationService, fileProcessService);
        ReflectionTestUtils.setField(service, "communityIndex", "community-index");
    }

    private JsonNode communityDetails(String communityId, String orgId, String communityName, String moderatorsJson) throws Exception {
        String json = "{\"communityId\":\"" + communityId + "\",\"orgId\":\"" + orgId
            + "\",\"communityName\":\"" + communityName + "\""
            + (moderatorsJson != null ? ",\"moderators\":" + moderatorsJson : "")
            + "}";
        return objectMapper.readTree(json);
    }

    private CommunityEntity entityWithData(String communityId, JsonNode data) {
        CommunityEntity entity = new CommunityEntity();
        entity.setCommunityId(communityId);
        entity.setData(data);
        entity.setActive(true);
        return entity;
    }

    // ---- publish ----

    @Test
    void publishReturnsBadRequestWhenUserIdBlank() throws Exception {
        when(accessTokenValidator.verifyUserToken("token")).thenReturn(null);

        ApiResponse response = service.publish(communityDetails("c1", "org1", "Name", null), "token");

        assertEquals(HttpStatus.BAD_REQUEST, response.getResponseCode());
    }

    @Test
    void publishReturnsBadRequestWhenPayloadValidationFails() throws Exception {
        when(accessTokenValidator.verifyUserToken("token")).thenReturn("user1");
        JsonNode details = communityDetails("c1", "org1", "Name", null);
        org.mockito.Mockito.doThrow(new CustomException(Constants.ERROR, "invalid payload", HttpStatus.BAD_REQUEST))
            .when(payloadValidation).validatePayload(eq(Constants.COMMUNITY_PUBLISH_PAYLOAD_VALIDATION_FILE), eq(details));

        ApiResponse response = service.publish(details, "token");

        assertEquals(HttpStatus.BAD_REQUEST, response.getResponseCode());
    }

    @Test
    void publishReturnsConflictWhenDuplicateCommunity() throws Exception {
        when(accessTokenValidator.verifyUserToken("token")).thenReturn("user1");
        JsonNode details = communityDetails("c1", "org1", "Name", null);
        when(esUtilService.isDuplicateCommunity("org1", "Name", "c1")).thenReturn(true);

        ApiResponse response = service.publish(details, "token");

        assertEquals(HttpStatus.CONFLICT, response.getResponseCode());
    }

    @Test
    void publishReturnsPreconditionFailedWhenNameConflictOnPublish() throws Exception {
        when(accessTokenValidator.verifyUserToken("token")).thenReturn("user1");
        JsonNode details = communityDetails("c1", "org1", "Name", null);
        when(esUtilService.isDuplicateCommunity("org1", "Name", "c1")).thenReturn(false);
        when(esUtilService.doesCommunityNameExistForPublish("Name", "c1")).thenReturn(true);

        ApiResponse response = service.publish(details, "token");

        assertEquals(HttpStatus.PRECONDITION_FAILED, response.getResponseCode());
    }

    @Test
    void publishReturnsBadRequestWhenCommunityNotFound() throws Exception {
        when(accessTokenValidator.verifyUserToken("token")).thenReturn("user1");
        JsonNode details = communityDetails("c1", "org1", "Name", null);
        when(esUtilService.isDuplicateCommunity(anyString(), anyString(), anyString())).thenReturn(false);
        when(esUtilService.doesCommunityNameExistForPublish(anyString(), anyString())).thenReturn(false);
        when(communityEngagementRepository.findByCommunityIdAndIsActive("c1", true)).thenReturn(Optional.empty());

        ApiResponse response = service.publish(details, "token");

        assertEquals(HttpStatus.BAD_REQUEST, response.getResponseCode());
    }

    @Test
    void publishSucceedsWithoutModeratorsAndSkipsNotification() throws Exception {
        when(accessTokenValidator.verifyUserToken("token")).thenReturn("user1");
        JsonNode details = communityDetails("c1", "org1", "Name", null);
        when(esUtilService.isDuplicateCommunity(anyString(), anyString(), anyString())).thenReturn(false);
        when(esUtilService.doesCommunityNameExistForPublish(anyString(), anyString())).thenReturn(false);
        JsonNode existingData = communityDetails("c1", "org1", "Name", null);
        when(communityEngagementRepository.findByCommunityIdAndIsActive("c1", true))
            .thenReturn(Optional.of(entityWithData("c1", existingData)));

        ApiResponse response = service.publish(details, "token");

        assertEquals(HttpStatus.OK, response.getResponseCode());
        verify(notificationService, never()).sendNotification(anyList(), anyString(), anyString(), anyString());
    }

    @Test
    void publishSucceedsWithModeratorsAndSendsNotification() throws Exception {
        when(accessTokenValidator.verifyUserToken("token")).thenReturn("user1");
        String moderatorsJson = "[{\"moderatorId\":\"mod1\"},{\"moderatorId\":\"mod2\"},{}]";
        JsonNode details = communityDetails("c1", "org1", "Name", moderatorsJson);
        when(esUtilService.isDuplicateCommunity(anyString(), anyString(), anyString())).thenReturn(false);
        when(esUtilService.doesCommunityNameExistForPublish(anyString(), anyString())).thenReturn(false);
        JsonNode existingData = communityDetails("c1", "org1", "Name", moderatorsJson);
        when(communityEngagementRepository.findByCommunityIdAndIsActive("c1", true))
            .thenReturn(Optional.of(entityWithData("c1", existingData)));

        ApiResponse response = service.publish(details, "token");

        assertEquals(HttpStatus.OK, response.getResponseCode());
        verify(notificationService, times(1)).sendNotification(
            eq(List.of("mod1", "mod2")), eq("c1"), eq("user1"), eq("Name"));
    }

    @Test
    void publishThrowsCustomExceptionOnUnexpectedError() throws Exception {
        when(accessTokenValidator.verifyUserToken("token")).thenReturn("user1");
        JsonNode details = communityDetails("c1", "org1", "Name", null);
        when(esUtilService.isDuplicateCommunity(anyString(), anyString(), anyString())).thenReturn(false);
        when(esUtilService.doesCommunityNameExistForPublish(anyString(), anyString())).thenReturn(false);
        when(communityEngagementRepository.findByCommunityIdAndIsActive("c1", true))
            .thenThrow(new RuntimeException("db down"));

        assertThrows(CustomException.class, () -> service.publish(details, "token"));
    }

    // ---- syncUserWithCommunity ----

    @Test
    void syncUserWithCommunityThrowsWhenFileNameNull() {
        org.springframework.web.multipart.MultipartFile file = mock(org.springframework.web.multipart.MultipartFile.class);
        when(file.getOriginalFilename()).thenReturn(null);

        assertThrows(CustomException.class, () -> service.syncUserWithCommunity(file));
    }

    @Test
    void syncUserWithCommunityThrowsWhenReadingFileFails() throws Exception {
        org.springframework.web.multipart.MultipartFile file = mock(org.springframework.web.multipart.MultipartFile.class);
        when(file.getOriginalFilename()).thenReturn("data.csv");
        when(file.getInputStream()).thenThrow(new java.io.IOException("disk error"));

        assertThrows(CustomException.class, () -> service.syncUserWithCommunity(file));
    }

    @Test
    void syncUserWithCommunityThrowsForUnsupportedFileType() {
        MockMultipartFile file = new MockMultipartFile("file", "data.txt", "text/plain", "data".getBytes());

        assertThrows(CustomException.class, () -> service.syncUserWithCommunity(file));
    }

    @Test
    void syncUserWithCommunityHandlesEmptyRecords() throws Exception {
        MockMultipartFile file = new MockMultipartFile("file", "data.csv", "text/csv", "data".getBytes());
        when(fileProcessService.processCsvAndSendMessage(any())).thenReturn(List.of());

        ApiResponse response = service.syncUserWithCommunity(file);

        assertEquals(HttpStatus.OK, response.getResponseCode());
        assertEquals(Constants.SUCCESS, response.getParams().getStatus());
    }

    @Test
    void syncUserWithCommunityProcessesTrueAndFalseStatusRecords() throws Exception {
        MockMultipartFile file = new MockMultipartFile("file", "data.csv", "text/csv", "data".getBytes());
        Map<String, String> trueRecord = new HashMap<>();
        trueRecord.put("status", "true");
        trueRecord.put(Constants.USER_ID_LOWER_CASE, "u1");
        trueRecord.put(Constants.COMMUNITY_ID_LOWERCASE, "c1");
        Map<String, String> falseRecord = new HashMap<>();
        falseRecord.put("status", "false");
        when(fileProcessService.processCsvAndSendMessage(any())).thenReturn(List.of(trueRecord, falseRecord));

        ApiResponse response = service.syncUserWithCommunity(file);

        assertEquals(HttpStatus.OK, response.getResponseCode());
        verify(esUtilService, times(1)).updateUserIndex("u1", "c1", true);
    }

    @Test
    void syncUserWithCommunityThrowsCustomExceptionOnProcessingError() throws Exception {
        MockMultipartFile file = new MockMultipartFile("file", "data.csv", "text/csv", "data".getBytes());
        when(fileProcessService.processCsvAndSendMessage(any())).thenThrow(new RuntimeException("bad csv"));

        assertThrows(CustomException.class, () -> service.syncUserWithCommunity(file));
    }
}
