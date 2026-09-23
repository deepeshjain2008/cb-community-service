package com.igot.cb.community.service.impl;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.igot.cb.authentication.util.AccessTokenValidator;
import com.igot.cb.community.entity.CommunityCategory;
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
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class CommunityManagementServiceImplCategoryTest {

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

    private static final String TOKEN = "auth-token";
    private static final String USER_ID = "user-1";

    @BeforeEach
    void setUp() {
        CbServerProperties cbServerProperties = new CbServerProperties();
        cbServerProperties.setJwtSecretKey("test-secret-key");
        cbServerProperties.setElasticCommunityCategoryJsonPath("/schema/category.json");
        cbServerProperties.setSearchResultRedisTtl(60L);
        service = new CommunityManagementServiceImpl(esUtilService, cacheService, objectMapper,
            cbServerProperties, payloadValidation, communityEngagementRepository, accessTokenValidator,
            cassandraOperation, redisTemplate, categoryRepository, producer, userService,
            notificationService, fileProcessService);
        ReflectionTestUtils.setField(service, "communityCategoryIndex", "community-category-index");
        ReflectionTestUtils.setField(service, "communityIndex", "community-index");
        lenient().when(accessTokenValidator.verifyUserToken(TOKEN)).thenReturn(USER_ID);
        lenient().when(redisTemplate.opsForValue()).thenReturn(valueOperations);
    }

    private ObjectNode categoryDetails(boolean withParent) {
        ObjectNode node = objectMapper.createObjectNode();
        node.put(Constants.CATEGORY_NAME, "Cat A");
        node.put(Constants.DESCRIPTION, "desc");
        if (withParent) {
            node.put(Constants.PARENT_ID, 1);
            node.put(Constants.DEPARTMENT_ID, "dept-1");
        }
        return node;
    }

    private List<Map<String, Object>> userRootOrgRecords(String rootOrgId) {
        Map<String, Object> row = new HashMap<>();
        row.put(Constants.USER_ROOT_ORG_ID, rootOrgId);
        return List.of(row);
    }

    private CommunityCategory savedCategory() {
        CommunityCategory category = new CommunityCategory();
        category.setCategoryId(10);
        category.setCategoryName("Cat A");
        return category;
    }

    // ---- categoryCreate ----

    @Test
    void categoryCreateReturnsBadRequestWhenUserIdBlank() {
        ApiResponse response = service.categoryCreate(categoryDetails(false), "bad-token");

        assertEquals(HttpStatus.BAD_REQUEST, response.getResponseCode());
    }

    @Test
    void categoryCreateReturnsBadRequestWhenUserDetailsNotFound() {
        when(cassandraOperation.getRecordsByPropertiesWithoutFiltering(
            eq(Constants.KEYSPACE_SUNBIRD), eq(Constants.TABLE_USER), any(Map.class), any(List.class), eq(2)))
            .thenReturn(List.of());

        ApiResponse response = service.categoryCreate(categoryDetails(false), TOKEN);

        assertEquals(HttpStatus.BAD_REQUEST, response.getResponseCode());
        assertEquals(Constants.USER_DETAILS_NOT_FOUND, response.getParams().getErrMsg());
    }

    @Test
    void categoryCreateReturnsBadRequestWhenPayloadInvalid() {
        when(cassandraOperation.getRecordsByPropertiesWithoutFiltering(
            eq(Constants.KEYSPACE_SUNBIRD), eq(Constants.TABLE_USER), any(Map.class), any(List.class), eq(2)))
            .thenReturn(userRootOrgRecords("org-1"));
        org.mockito.Mockito.doThrow(new CustomException(Constants.ERROR, "invalid category payload", HttpStatus.BAD_REQUEST))
            .when(payloadValidation).validatePayload(eq(Constants.CATEGORY_PAYLOAD_VALIDATION_FILE), any(JsonNode.class));

        ApiResponse response = service.categoryCreate(categoryDetails(false), TOKEN);

        assertEquals(HttpStatus.BAD_REQUEST, response.getResponseCode());
        assertEquals("invalid category payload", response.getParams().getErrMsg());
    }

    @Test
    void categoryCreateReturnsBadRequestWhenTopLevelCategoryAlreadyPresent() {
        when(cassandraOperation.getRecordsByPropertiesWithoutFiltering(
            eq(Constants.KEYSPACE_SUNBIRD), eq(Constants.TABLE_USER), any(Map.class), any(List.class), eq(2)))
            .thenReturn(userRootOrgRecords("org-1"));
        when(categoryRepository.findByCategoryNameAndIsActive("Cat A", true)).thenReturn(savedCategory());

        ApiResponse response = service.categoryCreate(categoryDetails(false), TOKEN);

        assertEquals(HttpStatus.BAD_REQUEST, response.getResponseCode());
        assertEquals(Constants.ALREADY_CATEGORY_PRESENT, response.getParams().getErrMsg());
    }

    @Test
    void categoryCreateCreatesTopLevelCategorySuccessfully() {
        when(cassandraOperation.getRecordsByPropertiesWithoutFiltering(
            eq(Constants.KEYSPACE_SUNBIRD), eq(Constants.TABLE_USER), any(Map.class), any(List.class), eq(2)))
            .thenReturn(userRootOrgRecords("org-1"));
        when(categoryRepository.findByCategoryNameAndIsActive("Cat A", true)).thenReturn(null);
        when(categoryRepository.save(any(CommunityCategory.class))).thenReturn(savedCategory());

        ApiResponse response = service.categoryCreate(categoryDetails(false), TOKEN);

        assertEquals(HttpStatus.OK, response.getResponseCode());
        assertEquals(10, response.getResult().get(Constants.CATEGORY_ID));
    }

    @Test
    void categoryCreateReturnsBadRequestWhenSubCategoryAlreadyPresentUnderTopic() {
        when(cassandraOperation.getRecordsByPropertiesWithoutFiltering(
            eq(Constants.KEYSPACE_SUNBIRD), eq(Constants.TABLE_USER), any(Map.class), any(List.class), eq(2)))
            .thenReturn(userRootOrgRecords("org-1"));
        when(categoryRepository.findByParentIdAndCategoryNameAndDepartmentIdAndIsActive(
            1, "Cat A", "dept-1", true)).thenReturn(savedCategory());

        ApiResponse response = service.categoryCreate(categoryDetails(true), TOKEN);

        assertEquals(HttpStatus.BAD_REQUEST, response.getResponseCode());
        assertEquals(Constants.ALREADY_PRESENT_COMMUNITY_UNDER_THIS_TOPIC, response.getParams().getErrMsg());
    }

    @Test
    void categoryCreateCreatesSubCategorySuccessfully() {
        when(cassandraOperation.getRecordsByPropertiesWithoutFiltering(
            eq(Constants.KEYSPACE_SUNBIRD), eq(Constants.TABLE_USER), any(Map.class), any(List.class), eq(2)))
            .thenReturn(userRootOrgRecords("org-1"));
        when(categoryRepository.findByParentIdAndCategoryNameAndDepartmentIdAndIsActive(
            1, "Cat A", "dept-1", true)).thenReturn(null);
        when(categoryRepository.save(any(CommunityCategory.class))).thenReturn(savedCategory());

        ApiResponse response = service.categoryCreate(categoryDetails(true), TOKEN);

        assertEquals(HttpStatus.OK, response.getResponseCode());
        assertEquals(10, response.getResult().get(Constants.CATEGORY_ID));
    }

    @Test
    void categoryCreateThrowsCustomExceptionOnUnexpectedError() {
        when(cassandraOperation.getRecordsByPropertiesWithoutFiltering(
            eq(Constants.KEYSPACE_SUNBIRD), eq(Constants.TABLE_USER), any(Map.class), any(List.class), eq(2)))
            .thenReturn(userRootOrgRecords("org-1"));
        when(categoryRepository.findByCategoryNameAndIsActive("Cat A", true))
            .thenThrow(new RuntimeException("db down"));

        JsonNode details = categoryDetails(false);
        assertThrows(CustomException.class, () -> service.categoryCreate(details, TOKEN));
    }

    // ---- readCategory ----

    @Test
    void readCategoryReturnsErrorWhenUserIdBlank() {
        ApiResponse response = service.readCategory("10", "bad-token");

        assertEquals(HttpStatus.INTERNAL_SERVER_ERROR, response.getResponseCode());
    }

    @Test
    void readCategoryReturnsErrorWhenCategoryIdEmpty() {
        ApiResponse response = service.readCategory("", TOKEN);

        assertEquals(HttpStatus.INTERNAL_SERVER_ERROR, response.getResponseCode());
    }

    @Test
    void readCategoryReturnsSuccessWhenFound() {
        when(categoryRepository.findByCategoryIdAndIsActive(10, true)).thenReturn(savedCategory());

        ApiResponse response = service.readCategory("10", TOKEN);

        assertEquals(Constants.SUCCESSFULLY_READING, response.getParams().getErrMsg());
    }

    @Test
    void readCategoryReturnsNotFoundWhenMissing() {
        when(categoryRepository.findByCategoryIdAndIsActive(10, true)).thenReturn(null);

        ApiResponse response = service.readCategory("10", TOKEN);

        assertEquals(HttpStatus.NOT_FOUND, response.getResponseCode());
        assertEquals(Constants.INVALID_CATEGORY_ID, response.getParams().getErrMsg());
    }

    @Test
    void readCategoryThrowsCustomExceptionOnUnexpectedError() {
        when(categoryRepository.findByCategoryIdAndIsActive(10, true))
            .thenThrow(new RuntimeException("db down"));

        assertThrows(CustomException.class, () -> service.readCategory("10", TOKEN));
    }

    // ---- deleteCategory ----

    @Test
    void deleteCategoryReturnsErrorWhenUserIdBlank() {
        ApiResponse response = service.deleteCategory("10", "bad-token");

        assertEquals(HttpStatus.INTERNAL_SERVER_ERROR, response.getResponseCode());
    }

    @Test
    void deleteCategoryReturnsErrorWhenCategoryIdEmpty() {
        ApiResponse response = service.deleteCategory("", TOKEN);

        assertEquals(HttpStatus.INTERNAL_SERVER_ERROR, response.getResponseCode());
    }

    @Test
    void deleteCategoryReturnsNotFoundWhenMissing() {
        when(categoryRepository.findByCategoryIdAndIsActive(10, true)).thenReturn(null);

        ApiResponse response = service.deleteCategory("10", TOKEN);

        assertEquals(HttpStatus.NOT_FOUND, response.getResponseCode());
    }

    @Test
    void deleteCategoryReturnsSuccessOnHappyPath() {
        when(categoryRepository.findByCategoryIdAndIsActive(10, true)).thenReturn(savedCategory());

        ApiResponse response = service.deleteCategory("10", TOKEN);

        assertEquals(true, ((String) response.getResult().get(Constants.RESPONSE)).contains("10"));
    }

    @Test
    void deleteCategoryThrowsCustomExceptionOnUnexpectedError() {
        when(categoryRepository.findByCategoryIdAndIsActive(10, true))
            .thenThrow(new RuntimeException("db down"));

        assertThrows(CustomException.class, () -> service.deleteCategory("10", TOKEN));
    }

    // ---- updateCategory ----

    @Test
    void updateCategoryReturnsErrorWhenUserIdBlank() {
        ApiResponse response = service.updateCategory(categoryDetails(false), "bad-token");

        assertEquals(HttpStatus.INTERNAL_SERVER_ERROR, response.getResponseCode());
    }

    @Test
    void updateCategoryReturnsBadRequestWhenPayloadInvalid() {
        org.mockito.Mockito.doThrow(new CustomException(Constants.ERROR, "bad payload", HttpStatus.BAD_REQUEST))
            .when(payloadValidation).validatePayload(eq(Constants.CATEGORY_PAYLOAD_VALIDATION_FILE), any(JsonNode.class));

        ApiResponse response = service.updateCategory(categoryDetails(false), TOKEN);

        assertEquals(HttpStatus.BAD_REQUEST, response.getResponseCode());
    }

    @Test
    void updateCategoryReturnsBadRequestWhenCategoryIdMissing() {
        ApiResponse response = service.updateCategory(categoryDetails(false), TOKEN);

        assertEquals(HttpStatus.BAD_REQUEST, response.getResponseCode());
        assertEquals(Constants.COMMUNITY_ID_NOT_FOUND, response.getParams().getErrMsg());
    }

    @Test
    void updateCategoryReturnsBadRequestWhenCategoryNotFound() {
        ObjectNode details = categoryDetails(false);
        details.put(Constants.CATEGORY_ID, 10);
        when(categoryRepository.findByCategoryIdAndIsActive(10, true)).thenReturn(null);

        ApiResponse response = service.updateCategory(details, TOKEN);

        assertEquals(HttpStatus.BAD_REQUEST, response.getResponseCode());
        assertEquals(Constants.INVALID_CATEGORY_ID, response.getParams().getErrMsg());
    }

    @Test
    void updateCategoryReturnsSuccessOnHappyPath() {
        ObjectNode details = categoryDetails(false);
        details.put(Constants.CATEGORY_ID, 10);
        when(categoryRepository.findByCategoryIdAndIsActive(10, true)).thenReturn(savedCategory());
        when(categoryRepository.save(any(CommunityCategory.class))).thenReturn(savedCategory());

        ApiResponse response = service.updateCategory(details, TOKEN);

        assertEquals(true, ((String) response.getResult().get(Constants.RESPONSE)).contains("10"));
    }

    @Test
    void updateCategoryThrowsCustomExceptionOnUnexpectedError() {
        ObjectNode details = categoryDetails(false);
        details.put(Constants.CATEGORY_ID, 10);
        when(categoryRepository.findByCategoryIdAndIsActive(10, true))
            .thenThrow(new RuntimeException("db down"));

        assertThrows(CustomException.class, () -> service.updateCategory(details, TOKEN));
    }

    // ---- listOfCategory ----

    @Test
    void listOfCategoryReturnsFromCache() {
        when(cacheService.getCache(Constants.CATEGORY_LIST_REDIS_KEY_PREFIX)).thenReturn("{\"a\":1}");

        ApiResponse response = service.listOfCategory();

        assertEquals(Constants.SUCCESSFULLY_READING, response.getParams().getErrMsg());
    }

    @Test
    void listOfCategoryReturnsBadRequestWhenNoneFound() {
        when(cacheService.getCache(Constants.CATEGORY_LIST_REDIS_KEY_PREFIX)).thenReturn(null);
        when(categoryRepository.findByParentIdAndIsActive(0, true)).thenReturn(List.of());

        ApiResponse response = service.listOfCategory();

        assertEquals(HttpStatus.BAD_REQUEST, response.getResponseCode());
    }

    @Test
    void listOfCategoryReturnsSuccessFromPrimary() {
        when(cacheService.getCache(Constants.CATEGORY_LIST_REDIS_KEY_PREFIX)).thenReturn(null);
        when(categoryRepository.findByParentIdAndIsActive(0, true)).thenReturn(List.of(savedCategory()));

        ApiResponse response = service.listOfCategory();

        assertEquals(HttpStatus.OK, response.getResponseCode());
    }

    @Test
    void listOfCategoryThrowsCustomExceptionOnUnexpectedError() {
        when(cacheService.getCache(Constants.CATEGORY_LIST_REDIS_KEY_PREFIX))
            .thenThrow(new RuntimeException("redis down"));

        assertThrows(CustomException.class, () -> service.listOfCategory());
    }

    // ---- listOfSubCategory ----

    private SearchCriteria subCategoryCriteria() {
        SearchCriteria criteria = new SearchCriteria();
        HashMap<String, Object> filterMap = new HashMap<>();
        filterMap.put(Constants.CATEGORY_ID, 10);
        criteria.setFilterCriteriaMap(filterMap);
        return criteria;
    }

    @Test
    void listOfSubCategoryReturnsBadRequestWhenCategoryIdMissingFromFilter() {
        SearchCriteria criteria = new SearchCriteria();
        criteria.setFilterCriteriaMap(new HashMap<>());

        ApiResponse response = service.listOfSubCategory(criteria);

        assertEquals(HttpStatus.BAD_REQUEST, response.getResponseCode());
    }

    @Test
    void listOfSubCategoryReturnsBadRequestWhenCategoryNotFound() {
        when(categoryRepository.findByCategoryIdAndIsActive(10, true)).thenReturn(null);

        ApiResponse response = service.listOfSubCategory(subCategoryCriteria());

        assertEquals(HttpStatus.BAD_REQUEST, response.getResponseCode());
        assertEquals(Constants.INVALID_CATEGORY_ID, response.getParams().getErrMsg());
    }

    @Test
    void listOfSubCategoryReturnsFromRedisCache() {
        when(categoryRepository.findByCategoryIdAndIsActive(10, true)).thenReturn(savedCategory());
        SearchResult cached = new SearchResult(objectMapper.createArrayNode(), Map.of(), 0L, List.of());
        when(valueOperations.get(any(String.class))).thenReturn(cached);

        ApiResponse response = service.listOfSubCategory(subCategoryCriteria());

        assertEquals(HttpStatus.OK, response.getResponseCode());
    }

    @Test
    void listOfSubCategoryReturnsBadRequestWhenSearchStringTooShort() {
        when(categoryRepository.findByCategoryIdAndIsActive(10, true)).thenReturn(savedCategory());
        when(valueOperations.get(any(String.class))).thenReturn(null);
        SearchCriteria criteria = subCategoryCriteria();
        criteria.setSearchString("a");

        ApiResponse response = service.listOfSubCategory(criteria);

        assertEquals(HttpStatus.BAD_REQUEST, response.getResponseCode());
    }

    @Test
    void listOfSubCategoryFetchesFromEsWhenNoCache() {
        when(categoryRepository.findByCategoryIdAndIsActive(10, true)).thenReturn(savedCategory());
        when(valueOperations.get(any(String.class))).thenReturn(null);
        SearchResult esResult = new SearchResult(objectMapper.createArrayNode(), Map.of(), 0L, List.of());
        when(esUtilService.searchDocuments(any(String.class), any(SearchCriteria.class))).thenReturn(esResult);

        ApiResponse response = service.listOfSubCategory(subCategoryCriteria());

        assertEquals(HttpStatus.OK, response.getResponseCode());
    }

    @Test
    void listOfSubCategoryThrowsCustomExceptionOnUnexpectedError() {
        when(categoryRepository.findByCategoryIdAndIsActive(10, true))
            .thenThrow(new RuntimeException("db down"));

        SearchCriteria criteria = subCategoryCriteria();
        assertThrows(CustomException.class, () -> service.listOfSubCategory(criteria));
    }

    // ---- lisAllCategoryWithSubCat ----

    @Test
    void lisAllCategoryWithSubCatReturnsFromCache() {
        when(cacheService.getCache(Constants.CATEGORY_LIST_ALL_REDIS_KEY_PREFIX)).thenReturn("{\"a\":1}");

        ApiResponse response = service.lisAllCategoryWithSubCat();

        assertEquals(Constants.SUCCESSFULLY_READING, response.getParams().getErrMsg());
    }

    @Test
    void lisAllCategoryWithSubCatReturnsNotFoundWhenNoTopLevelCategories() throws Exception {
        when(cacheService.getCache(Constants.CATEGORY_LIST_ALL_REDIS_KEY_PREFIX)).thenReturn(null);
        when(categoryRepository.findByParentIdAndIsActive(0, true)).thenReturn(List.of());
        SearchResult emptyResult = new SearchResult(objectMapper.createArrayNode(), Map.of(), 0L, List.of());
        when(esUtilService.fetchTopCommunitiesForTopics(any(List.class), any(String.class))).thenReturn(emptyResult);

        ApiResponse response = service.lisAllCategoryWithSubCat();

        assertEquals(HttpStatus.NOT_FOUND, response.getResponseCode());
    }

    @Test
    void lisAllCategoryWithSubCatReturnsSuccessWithData() throws Exception {
        when(cacheService.getCache(Constants.CATEGORY_LIST_ALL_REDIS_KEY_PREFIX)).thenReturn(null);
        when(categoryRepository.findByParentIdAndIsActive(0, true)).thenReturn(List.of(savedCategory()));
        ObjectNode doc = objectMapper.createObjectNode();
        doc.put(Constants.ORD_ID, "org-1");
        doc.put(Constants.TOPIC_ID, 10);
        com.fasterxml.jackson.databind.node.ArrayNode data = objectMapper.createArrayNode();
        data.add(doc);
        SearchResult searchResult = new SearchResult(data, Map.of(), 1L, List.of());
        when(esUtilService.fetchTopCommunitiesForTopics(any(List.class), any(String.class))).thenReturn(searchResult);
        when(cassandraOperation.getRecordsByPropertiesWithoutFiltering(
            eq(Constants.KEYSPACE_SUNBIRD), eq(Constants.TABLE_ORGANISATION), any(Map.class), any(List.class), eq(null)))
            .thenReturn(List.of());

        ApiResponse response = service.lisAllCategoryWithSubCat();

        assertEquals(HttpStatus.OK, response.getResponseCode());
    }

    @Test
    void lisAllCategoryWithSubCatThrowsCustomExceptionOnUnexpectedError() {
        when(cacheService.getCache(Constants.CATEGORY_LIST_ALL_REDIS_KEY_PREFIX))
            .thenThrow(new RuntimeException("redis down"));

        assertThrows(CustomException.class, () -> service.lisAllCategoryWithSubCat());
    }
}
