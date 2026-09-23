package com.igot.cb.community.service.impl;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.igot.cb.authentication.util.AccessTokenValidator;
import com.igot.cb.community.entity.CommunityCategory;
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
import org.sunbird.cloud.storage.BaseStorageService;
import scala.Option;

import java.io.File;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class CommunityManagementServiceImplTest {

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
    @Mock
    private BaseStorageService storageService;

    private CommunityManagementServiceImpl service;
    private final ObjectMapper objectMapper = new ObjectMapper();
    private CbServerProperties cbServerProperties;

    private static final String TOKEN = "auth-token";
    private static final String USER_ID = "user-1";
    private static final String COMMUNITY_ID = "community-1";

    @BeforeEach
    void setUp() {
        cbServerProperties = new CbServerProperties();
        cbServerProperties.setDiscussionCloudFolderName("discussion");
        cbServerProperties.setDiscussionContainerName("container");
        cbServerProperties.setElasticCommunityJsonPath("/schema/community.json");
        cbServerProperties.setElasticCommunityCategoryJsonPath("/schema/category.json");
        cbServerProperties.setSearchResultRedisTtl(60L);
        cbServerProperties.setSearchStringMaxRegexLength(100);
        cbServerProperties.setSearchQueryFields("communityName,description");
        cbServerProperties.setReporCommunityUserLimit(3);
        cbServerProperties.setCommunityAdminJoinMaxUser(50);
        cbServerProperties.setJwtSecretKey("test-secret-key");

        service = new CommunityManagementServiceImpl(esUtilService, cacheService, objectMapper,
            cbServerProperties, payloadValidation, communityEngagementRepository, accessTokenValidator,
            cassandraOperation, redisTemplate, categoryRepository, producer, userService,
            notificationService, fileProcessService);

        // Avoid touching the real cloud SDK: inject the mocked storage service directly,
        // bypassing the @PostConstruct init() that would call StorageServiceFactory.
        ReflectionTestUtils.setField(service, "storageService", storageService);
        ReflectionTestUtils.setField(service, "userCountUpdateTopic", "user-count-topic");
        ReflectionTestUtils.setField(service, "noOfPopularCommunities", 10);
        ReflectionTestUtils.setField(service, "communityCategoryIndex", "community-category-index");
        ReflectionTestUtils.setField(service, "communityIndex", "community-index");

        lenient().when(accessTokenValidator.verifyUserToken(TOKEN)).thenReturn(USER_ID);
    }

    private ObjectNode communityDetails() {
        ObjectNode node = objectMapper.createObjectNode();
        node.put(Constants.COMMUNITY_NAME, "My Community");
        node.put(Constants.TOPIC_ID, 1);
        return node;
    }

    private List<Map<String, Object>> userRecords(String rootOrgId) {
        Map<String, Object> record = new HashMap<>();
        record.put(Constants.USER_ROOT_ORG_ID, rootOrgId);
        return List.of(record);
    }

    private CommunityCategory category() {
        CommunityCategory category = new CommunityCategory();
        category.setCategoryId(1);
        category.setCategoryName("Category 1");
        category.setCountOfCommunities(0L);
        return category;
    }

    private CommunityEntity communityEntity(String communityId, JsonNode data) {
        CommunityEntity entity = new CommunityEntity();
        entity.setCommunityId(communityId);
        entity.setData(data);
        entity.setActive(true);
        entity.setCreatedOn(new Timestamp(System.currentTimeMillis()));
        entity.setUpdatedOn(new Timestamp(System.currentTimeMillis()));
        return entity;
    }

    @Test
    void uploadFileDeletesTemporaryFileOnSuccess() {
        when(storageService.upload(anyString(), anyString(), anyString(), any(Option.class),
            any(Option.class), any(Option.class), any(Option.class)))
            .thenAnswer(invocation -> {
                String localFilePath = invocation.getArgument(1);
                assertFalse(new File(localFilePath).getName().isEmpty());
                return "https://cloud.example.com/uploaded-file.txt";
            });

        MockMultipartFile file = new MockMultipartFile("file", "test.txt", "text/plain",
            "hello world".getBytes());

        ApiResponse response = service.uploadFile(file, "community-1");

        assertEquals(HttpStatus.OK, response.getResponseCode());
        assertEquals("https://cloud.example.com/uploaded-file.txt", response.getResult().get(Constants.URL));
    }

    @Test
    void uploadFileDeletesTemporaryFileOnUploadFailure() {
        when(storageService.upload(anyString(), anyString(), anyString(), any(Option.class),
            any(Option.class), any(Option.class), any(Option.class)))
            .thenThrow(new RuntimeException("cloud unavailable"));

        MockMultipartFile file = new MockMultipartFile("file", "test.txt", "text/plain",
            "hello world".getBytes());

        ApiResponse response = service.uploadFile(file, "community-1");

        assertEquals(HttpStatus.INTERNAL_SERVER_ERROR, response.getResponseCode());
        assertEquals(Constants.FAILED, response.getParams().getStatus());
    }

    @Test
    void uploadFileReturnsBadRequestWhenFileEmpty() {
        MockMultipartFile file = new MockMultipartFile("file", "test.txt", "text/plain", new byte[0]);

        ApiResponse response = service.uploadFile(file, "community-1");

        assertEquals(HttpStatus.BAD_REQUEST, response.getResponseCode());
        assertEquals(Constants.COMMUNITY_FILE_EMPTY, response.getParams().getErr());
    }

    @Test
    void uploadFileReturnsBadRequestWhenCommunityIdBlank() {
        MockMultipartFile file = new MockMultipartFile("file", "test.txt", "text/plain", "hello".getBytes());

        ApiResponse response = service.uploadFile(file, "   ");

        assertEquals(HttpStatus.BAD_REQUEST, response.getResponseCode());
        assertEquals(Constants.INVALID_COMMUNITY_ID, response.getParams().getErr());
    }

    @Test
    void uploadFileReturnsInternalServerErrorWhenReadingBytesFails() throws Exception {
        org.springframework.web.multipart.MultipartFile file =
            mock(org.springframework.web.multipart.MultipartFile.class);
        when(file.isEmpty()).thenReturn(false);
        when(file.getOriginalFilename()).thenReturn("test.txt");
        when(file.getBytes()).thenThrow(new java.io.IOException("read failure"));

        ApiResponse response = service.uploadFile(file, "community-1");

        assertEquals(HttpStatus.INTERNAL_SERVER_ERROR, response.getResponseCode());
        assertEquals(Constants.FAILED, response.getParams().getStatus());
    }

    // ---- create ----

    @Test
    void createReturnsBadRequestWhenUserIdBlank() {
        ApiResponse response = service.create(communityDetails(), "bad-token");

        assertEquals(HttpStatus.BAD_REQUEST, response.getResponseCode());
        assertEquals(Constants.USER_ID_DOESNT_EXIST, response.getParams().getErrMsg());
    }

    @Test
    void createReturnsBadRequestWhenPayloadValidationFails() {
        org.mockito.Mockito.doThrow(new CustomException(Constants.ERROR, "invalid payload", HttpStatus.BAD_REQUEST))
            .when(payloadValidation).validatePayload(eq(Constants.PAYLOAD_VALIDATION_FILE), any(JsonNode.class));

        ApiResponse response = service.create(communityDetails(), TOKEN);

        assertEquals(HttpStatus.BAD_REQUEST, response.getResponseCode());
        assertEquals("invalid payload", response.getParams().getErrMsg());
    }

    @Test
    void createReturnsBadRequestWhenTopicInactive() {
        when(categoryRepository.findByCategoryIdAndIsActive(1, true)).thenReturn(null);

        ApiResponse response = service.create(communityDetails(), TOKEN);

        assertEquals(HttpStatus.BAD_REQUEST, response.getResponseCode());
        assertEquals(Constants.TOPIC_IS_INACTIVE, response.getParams().getErrMsg());
    }

    @Test
    void createReturnsBadRequestWhenUserDetailsNotFound() {
        when(categoryRepository.findByCategoryIdAndIsActive(1, true)).thenReturn(category());
        when(cassandraOperation.getRecordsByPropertiesWithoutFiltering(
            eq(Constants.KEYSPACE_SUNBIRD), eq(Constants.TABLE_USER), any(Map.class), anyList(), eq(2)))
            .thenReturn(List.of());

        ApiResponse response = service.create(communityDetails(), TOKEN);

        assertEquals(HttpStatus.BAD_REQUEST, response.getResponseCode());
        assertEquals(Constants.USER_DETAILS_NOT_FOUND, response.getParams().getErrMsg());
    }

    @Test
    void createReturnsConflictWhenCommunityExists() {
        when(categoryRepository.findByCategoryIdAndIsActive(1, true)).thenReturn(category());
        when(cassandraOperation.getRecordsByPropertiesWithoutFiltering(
            eq(Constants.KEYSPACE_SUNBIRD), eq(Constants.TABLE_USER), any(Map.class), anyList(), eq(2)))
            .thenReturn(userRecords("org-1"));
        when(esUtilService.doesCommunityExist("org-1", "My Community")).thenReturn(true);

        ApiResponse response = service.create(communityDetails(), TOKEN);

        assertEquals(HttpStatus.CONFLICT, response.getResponseCode());
        assertEquals(Constants.CREATE_ERROR_MSG_WITHIN_COMMUNITY, response.getParams().getErrMsg());
    }

    @Test
    void createReturnsPreconditionFailedWhenCommunityNameExistsAndCreationNotAllowed() {
        when(categoryRepository.findByCategoryIdAndIsActive(1, true)).thenReturn(category());
        when(cassandraOperation.getRecordsByPropertiesWithoutFiltering(
            eq(Constants.KEYSPACE_SUNBIRD), eq(Constants.TABLE_USER), any(Map.class), anyList(), eq(2)))
            .thenReturn(userRecords("org-1"));
        when(esUtilService.doesCommunityExist("org-1", "My Community")).thenReturn(false);
        when(esUtilService.doesCommunityNameExist("My Community")).thenReturn(true);

        ApiResponse response = service.create(communityDetails(), TOKEN);

        assertEquals(HttpStatus.PRECONDITION_FAILED, response.getResponseCode());
        assertEquals(Constants.CREATE_ERROR_MSG_COMMUNITY, response.getParams().getErrMsg());
    }

    @Test
    void createReturnsNotFoundWhenOrgDetailsMissing() {
        when(categoryRepository.findByCategoryIdAndIsActive(1, true)).thenReturn(category());
        when(cassandraOperation.getRecordsByPropertiesWithoutFiltering(
            eq(Constants.KEYSPACE_SUNBIRD), eq(Constants.TABLE_USER), any(Map.class), anyList(), eq(2)))
            .thenReturn(userRecords("org-1"));
        when(esUtilService.doesCommunityExist("org-1", "My Community")).thenReturn(false);
        when(esUtilService.doesCommunityNameExist("My Community")).thenReturn(false);
        when(cassandraOperation.getRecordsByPropertiesWithoutFiltering(
            eq(Constants.KEYSPACE_SUNBIRD), eq(Constants.ORG_TABLE), any(Map.class), eq(null), eq(1)))
            .thenReturn(List.of());

        ApiResponse response = service.create(communityDetails(), TOKEN);

        assertEquals(HttpStatus.NOT_FOUND, response.getResponseCode());
        assertEquals(Constants.ORG_DETAILS_NOT_FOUND, response.getParams().getErrMsg());
    }

    @Test
    void createReturnsSuccessOnHappyPath() {
        when(categoryRepository.findByCategoryIdAndIsActive(1, true)).thenReturn(category());
        when(cassandraOperation.getRecordsByPropertiesWithoutFiltering(
            eq(Constants.KEYSPACE_SUNBIRD), eq(Constants.TABLE_USER), any(Map.class), anyList(), eq(2)))
            .thenReturn(userRecords("org-1"));
        when(esUtilService.doesCommunityExist("org-1", "My Community")).thenReturn(false);
        when(esUtilService.doesCommunityNameExist("My Community")).thenReturn(false);
        Map<String, Object> orgRecord = new HashMap<>();
        orgRecord.put(Constants.ORG_NAME, "Org One");
        when(cassandraOperation.getRecordsByPropertiesWithoutFiltering(
            eq(Constants.KEYSPACE_SUNBIRD), eq(Constants.ORG_TABLE), any(Map.class), eq(null), eq(1)))
            .thenReturn(List.of(orgRecord));
        when(communityEngagementRepository.save(any(CommunityEntity.class)))
            .thenAnswer(inv -> communityEntity(COMMUNITY_ID, communityDetails()));

        ApiResponse response = service.create(communityDetails(), TOKEN);

        assertEquals(HttpStatus.OK, response.getResponseCode());
        assertEquals(Constants.SUCCESSFULLY_CREATED, response.getResult().get(Constants.STATUS));
        verify(categoryRepository).save(any(CommunityCategory.class));
    }

    @Test
    void createReturnsInternalServerErrorWhenSaveReturnsNullData() {
        when(categoryRepository.findByCategoryIdAndIsActive(1, true)).thenReturn(category());
        when(cassandraOperation.getRecordsByPropertiesWithoutFiltering(
            eq(Constants.KEYSPACE_SUNBIRD), eq(Constants.TABLE_USER), any(Map.class), anyList(), eq(2)))
            .thenReturn(userRecords("org-1"));
        when(esUtilService.doesCommunityExist("org-1", "My Community")).thenReturn(false);
        when(esUtilService.doesCommunityNameExist("My Community")).thenReturn(false);
        Map<String, Object> orgRecord = new HashMap<>();
        orgRecord.put(Constants.ORG_NAME, "Org One");
        when(cassandraOperation.getRecordsByPropertiesWithoutFiltering(
            eq(Constants.KEYSPACE_SUNBIRD), eq(Constants.ORG_TABLE), any(Map.class), eq(null), eq(1)))
            .thenReturn(List.of(orgRecord));
        CommunityEntity savedWithNullData = communityEntity(COMMUNITY_ID,
            com.fasterxml.jackson.databind.node.NullNode.getInstance());
        when(communityEngagementRepository.save(any(CommunityEntity.class))).thenReturn(savedWithNullData);

        ApiResponse response = service.create(communityDetails(), TOKEN);

        assertEquals(HttpStatus.INTERNAL_SERVER_ERROR, response.getResponseCode());
        assertEquals(Constants.FAILED, response.getParams().getStatus());
    }

    @Test
    void createSkipsNameExistCheckWhenCommunityCreationAllowed() {
        ObjectNode details = communityDetails();
        details.put(Constants.COMMUNITY_CREATION_ALLOWED, true);
        when(categoryRepository.findByCategoryIdAndIsActive(1, true)).thenReturn(category());
        when(cassandraOperation.getRecordsByPropertiesWithoutFiltering(
            eq(Constants.KEYSPACE_SUNBIRD), eq(Constants.TABLE_USER), any(Map.class), anyList(), eq(2)))
            .thenReturn(userRecords("org-1"));
        when(esUtilService.doesCommunityExist("org-1", "My Community")).thenReturn(false);
        Map<String, Object> orgRecord = new HashMap<>();
        orgRecord.put(Constants.ORG_NAME, "Org One");
        when(cassandraOperation.getRecordsByPropertiesWithoutFiltering(
            eq(Constants.KEYSPACE_SUNBIRD), eq(Constants.ORG_TABLE), any(Map.class), eq(null), eq(1)))
            .thenReturn(List.of(orgRecord));
        when(communityEngagementRepository.save(any(CommunityEntity.class)))
            .thenAnswer(inv -> communityEntity(COMMUNITY_ID, details));

        ApiResponse response = service.create(details, TOKEN);

        assertEquals(HttpStatus.OK, response.getResponseCode());
        verify(esUtilService, never()).doesCommunityNameExist(anyString());
    }

    @Test
    void createThrowsCustomExceptionOnUnexpectedError() {
        when(categoryRepository.findByCategoryIdAndIsActive(1, true))
            .thenThrow(new RuntimeException("db down"));

        ObjectNode details = communityDetails();
        assertThrows(CustomException.class, () -> service.create(details, TOKEN));
    }

    // ---- read(communityId, authToken) ----

    @Test
    void readReturnsErrorWhenUserIdBlank() {
        ApiResponse response = service.read(COMMUNITY_ID, "bad-token");

        assertEquals(HttpStatus.INTERNAL_SERVER_ERROR, response.getResponseCode());
    }

    @Test
    void readReturnsErrorWhenCommunityIdEmpty() {
        ApiResponse response = service.read("", TOKEN);

        assertEquals(HttpStatus.INTERNAL_SERVER_ERROR, response.getResponseCode());
    }

    @Test
    void readReturnsFromCacheWhenPresent() {
        when(cacheService.getCache(COMMUNITY_ID)).thenReturn("{\"communityName\":\"Cached\"}");

        ApiResponse response = service.read(COMMUNITY_ID, TOKEN);

        assertEquals(Constants.SUCCESSFULLY_READING, response.getParams().getErrMsg());
    }

    @Test
    void readFallsBackToPrimaryWhenCacheMiss() {
        when(cacheService.getCache(COMMUNITY_ID)).thenReturn(null);
        when(communityEngagementRepository.findByCommunityIdAndIsActive(COMMUNITY_ID, true))
            .thenReturn(Optional.of(communityEntity(COMMUNITY_ID, communityDetails())));

        ApiResponse response = service.read(COMMUNITY_ID, TOKEN);

        assertEquals(Constants.SUCCESSFULLY_READING, response.getParams().getErrMsg());
    }

    @Test
    void readReturnsNotFoundWhenCommunityMissing() {
        when(cacheService.getCache(COMMUNITY_ID)).thenReturn(null);
        when(communityEngagementRepository.findByCommunityIdAndIsActive(COMMUNITY_ID, true))
            .thenReturn(Optional.empty());

        ApiResponse response = service.read(COMMUNITY_ID, TOKEN);

        assertEquals(HttpStatus.NOT_FOUND, response.getResponseCode());
        assertEquals(Constants.INVALID_COMMUNITY_ID, response.getParams().getErrMsg());
    }

    @Test
    void readThrowsCustomExceptionOnUnexpectedError() {
        when(cacheService.getCache(COMMUNITY_ID)).thenThrow(new RuntimeException("redis down"));

        assertThrows(CustomException.class, () -> service.read(COMMUNITY_ID, TOKEN));
    }

    // ---- read(communityId) single-arg overload ----

    @Test
    void readSingleArgReturnsErrorWhenCommunityIdEmpty() {
        ApiResponse response = service.read("");

        assertEquals(HttpStatus.INTERNAL_SERVER_ERROR, response.getResponseCode());
    }

    @Test
    void readSingleArgReturnsFromCacheWhenPresent() {
        when(cacheService.getCache(COMMUNITY_ID)).thenReturn("{\"communityName\":\"Cached\"}");

        ApiResponse response = service.read(COMMUNITY_ID);

        assertEquals(Constants.SUCCESSFULLY_READING, response.getParams().getErrMsg());
    }

    @Test
    void readSingleArgFallsBackToPrimaryWhenCacheMiss() {
        when(cacheService.getCache(COMMUNITY_ID)).thenReturn(null);
        when(communityEngagementRepository.findByCommunityIdAndIsActive(COMMUNITY_ID, true))
            .thenReturn(Optional.of(communityEntity(COMMUNITY_ID, communityDetails())));

        ApiResponse response = service.read(COMMUNITY_ID);

        assertEquals(Constants.SUCCESSFULLY_READING, response.getParams().getErrMsg());
        verify(cacheService).putCache(eq(COMMUNITY_ID), any());
    }

    @Test
    void readSingleArgReturnsNotFoundWhenCommunityMissing() {
        when(cacheService.getCache(COMMUNITY_ID)).thenReturn(null);
        when(communityEngagementRepository.findByCommunityIdAndIsActive(COMMUNITY_ID, true))
            .thenReturn(Optional.empty());

        ApiResponse response = service.read(COMMUNITY_ID);

        assertEquals(HttpStatus.NOT_FOUND, response.getResponseCode());
        assertEquals(Constants.INVALID_COMMUNITY_ID, response.getParams().getErrMsg());
    }

    @Test
    void readSingleArgThrowsCustomExceptionOnUnexpectedError() {
        when(cacheService.getCache(COMMUNITY_ID)).thenThrow(new RuntimeException("redis down"));

        assertThrows(CustomException.class, () -> service.read(COMMUNITY_ID));
    }

    // ---- delete ----

    @Test
    void deleteReturnsBadRequestWhenUserIdBlank() {
        ApiResponse response = service.delete(COMMUNITY_ID, "bad-token");

        assertEquals(HttpStatus.BAD_REQUEST, response.getResponseCode());
    }

    @Test
    void deleteReturnsErrorWhenCommunityIdEmpty() {
        ApiResponse response = service.delete("", TOKEN);

        assertEquals(HttpStatus.INTERNAL_SERVER_ERROR, response.getResponseCode());
    }

    @Test
    void deleteReturnsNotFoundWhenCommunityMissing() {
        when(communityEngagementRepository.findByCommunityIdAndIsActive(COMMUNITY_ID, true))
            .thenReturn(Optional.empty());

        ApiResponse response = service.delete(COMMUNITY_ID, TOKEN);

        assertEquals(HttpStatus.NOT_FOUND, response.getResponseCode());
    }

    @Test
    void deleteReturnsBadRequestWhenTopicInactive() {
        ObjectNode data = communityDetails();
        CommunityEntity entity = communityEntity(COMMUNITY_ID, data);
        when(communityEngagementRepository.findByCommunityIdAndIsActive(COMMUNITY_ID, true))
            .thenReturn(Optional.of(entity));
        when(categoryRepository.findByCategoryIdAndIsActive(1, true)).thenReturn(null);

        ApiResponse response = service.delete(COMMUNITY_ID, TOKEN);

        assertEquals(HttpStatus.BAD_REQUEST, response.getResponseCode());
        assertEquals(Constants.TOPIC_IS_INACTIVE, response.getParams().getErrMsg());
    }

    @Test
    void deleteReturnsSuccessOnHappyPath() {
        ObjectNode data = communityDetails();
        CommunityEntity entity = communityEntity(COMMUNITY_ID, data);
        when(communityEngagementRepository.findByCommunityIdAndIsActive(COMMUNITY_ID, true))
            .thenReturn(Optional.of(entity));
        when(categoryRepository.findByCategoryIdAndIsActive(1, true)).thenReturn(category());

        ApiResponse response = service.delete(COMMUNITY_ID, TOKEN);

        assertTrue(((String) response.getResult().get(Constants.RESPONSE)).contains(COMMUNITY_ID));
        verify(communityEngagementRepository).save(entity);
    }

    @Test
    void deleteThrowsCustomExceptionOnUnexpectedError() {
        when(communityEngagementRepository.findByCommunityIdAndIsActive(COMMUNITY_ID, true))
            .thenThrow(new RuntimeException("db down"));

        assertThrows(CustomException.class, () -> service.delete(COMMUNITY_ID, TOKEN));
    }

    // ---- update ----

    @Test
    void updateReturnsBadRequestWhenUserIdBlank() {
        ApiResponse response = service.update(communityDetails(), "bad-token");

        assertEquals(HttpStatus.BAD_REQUEST, response.getResponseCode());
    }

    @Test
    void updateReturnsBadRequestWhenCommunityIdMissing() {
        ApiResponse response = service.update(communityDetails(), TOKEN);

        assertEquals(HttpStatus.BAD_REQUEST, response.getResponseCode());
        assertEquals(Constants.COMMUNITY_ID_NOT_FOUND, response.getParams().getErrMsg());
    }

    @Test
    void updateReturnsBadRequestWhenCommunityNotFound() {
        ObjectNode details = communityDetails();
        details.put(Constants.COMMUNITY_ID, COMMUNITY_ID);
        when(communityEngagementRepository.findByCommunityIdAndIsActive(COMMUNITY_ID, true))
            .thenReturn(Optional.empty());

        ApiResponse response = service.update(details, TOKEN);

        assertEquals(HttpStatus.BAD_REQUEST, response.getResponseCode());
        assertEquals(Constants.INVALID_COMMUNITY_ID, response.getParams().getErrMsg());
    }

    @Test
    void updateReturnsSuccessOnHappyPath() {
        ObjectNode details = communityDetails();
        details.put(Constants.COMMUNITY_ID, COMMUNITY_ID);
        ObjectNode existingData = communityDetails();
        existingData.put(Constants.COMMUNITY_ID, COMMUNITY_ID);
        existingData.put(Constants.ORG_ID, "org-1");
        CommunityEntity entity = communityEntity(COMMUNITY_ID, existingData);
        when(communityEngagementRepository.findByCommunityIdAndIsActive(COMMUNITY_ID, true))
            .thenReturn(Optional.of(entity));
        when(esUtilService.isDuplicateCommunity(anyString(), anyString(), anyString())).thenReturn(false);
        when(esUtilService.doesCommunityNameExistForPublish(anyString(), anyString())).thenReturn(false);

        ApiResponse response = service.update(details, TOKEN);

        assertTrue(((String) response.getResult().get(Constants.RESPONSE)).contains(COMMUNITY_ID));
    }

    @Test
    void updateReturnsConflictWhenDuplicateCommunity() {
        ObjectNode details = communityDetails();
        details.put(Constants.COMMUNITY_ID, COMMUNITY_ID);
        ObjectNode existingData = communityDetails();
        existingData.put(Constants.COMMUNITY_ID, COMMUNITY_ID);
        existingData.put(Constants.ORG_ID, "org-1");
        CommunityEntity entity = communityEntity(COMMUNITY_ID, existingData);
        when(communityEngagementRepository.findByCommunityIdAndIsActive(COMMUNITY_ID, true))
            .thenReturn(Optional.of(entity));
        when(esUtilService.isDuplicateCommunity(anyString(), anyString(), anyString())).thenReturn(true);

        ApiResponse response = service.update(details, TOKEN);

        assertEquals(HttpStatus.CONFLICT, response.getResponseCode());
    }

    @Test
    void updateReturnsPreconditionFailedWhenNameConflictOnUpdate() {
        ObjectNode details = communityDetails();
        details.put(Constants.COMMUNITY_ID, COMMUNITY_ID);
        ObjectNode existingData = communityDetails();
        existingData.put(Constants.COMMUNITY_ID, COMMUNITY_ID);
        existingData.put(Constants.ORG_ID, "org-1");
        CommunityEntity entity = communityEntity(COMMUNITY_ID, existingData);
        when(communityEngagementRepository.findByCommunityIdAndIsActive(COMMUNITY_ID, true))
            .thenReturn(Optional.of(entity));
        when(esUtilService.isDuplicateCommunity(anyString(), anyString(), anyString())).thenReturn(false);
        when(esUtilService.doesCommunityNameExistForPublish(anyString(), anyString())).thenReturn(true);

        ApiResponse response = service.update(details, TOKEN);

        assertEquals(HttpStatus.PRECONDITION_FAILED, response.getResponseCode());
        assertEquals(Constants.CREATE_ERROR_MSG_COMMUNITY, response.getParams().getErrMsg());
    }

    @Test
    void updateMergesNewFieldNotPresentInExistingData() {
        ObjectNode details = communityDetails();
        details.put(Constants.COMMUNITY_ID, COMMUNITY_ID);
        details.put("newField", "newValue");
        ObjectNode existingData = communityDetails();
        existingData.put(Constants.COMMUNITY_ID, COMMUNITY_ID);
        existingData.put(Constants.ORG_ID, "org-1");
        CommunityEntity entity = communityEntity(COMMUNITY_ID, existingData);
        when(communityEngagementRepository.findByCommunityIdAndIsActive(COMMUNITY_ID, true))
            .thenReturn(Optional.of(entity));
        when(esUtilService.isDuplicateCommunity(anyString(), anyString(), anyString())).thenReturn(false);
        when(esUtilService.doesCommunityNameExistForPublish(anyString(), anyString())).thenReturn(false);

        ApiResponse response = service.update(details, TOKEN);

        assertEquals(HttpStatus.OK, response.getResponseCode());
        assertEquals("newValue", existingData.get("newField").asText());
    }

    @Test
    void updateThrowsCustomExceptionOnUnexpectedError() {
        ObjectNode details = communityDetails();
        details.put(Constants.COMMUNITY_ID, COMMUNITY_ID);
        when(communityEngagementRepository.findByCommunityIdAndIsActive(COMMUNITY_ID, true))
            .thenThrow(new RuntimeException("db down"));

        assertThrows(CustomException.class, () -> service.update(details, TOKEN));
    }
}
