package com.igot.cb.community.service.impl;

import co.elastic.clients.elasticsearch._types.SortOptions;
import co.elastic.clients.elasticsearch._types.SortOrder;
import co.elastic.clients.elasticsearch._types.aggregations.Aggregate;
import co.elastic.clients.elasticsearch._types.aggregations.Aggregation;
import co.elastic.clients.elasticsearch._types.aggregations.StringTermsAggregate;
import co.elastic.clients.elasticsearch._types.query_dsl.Query;
import co.elastic.clients.elasticsearch.core.SearchRequest;
import co.elastic.clients.elasticsearch.core.SearchResponse;
import co.elastic.clients.elasticsearch.core.search.Hit;
import com.auth0.jwt.JWT;
import com.auth0.jwt.algorithms.Algorithm;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.igot.cb.authentication.util.AccessTokenValidator;
import com.igot.cb.community.entity.CommunityCategory;
import com.igot.cb.community.entity.CommunityEntity;
import com.igot.cb.community.kafka.producer.Producer;
import com.igot.cb.community.repository.CommunityCategoryRepository;
import com.igot.cb.community.repository.CommunityEngagementRepository;
import com.igot.cb.community.service.CommunityManagementService;
import com.igot.cb.community.service.NotificationService;
import com.igot.cb.community.service.UserService;
import com.igot.cb.pores.cache.CacheService;
import com.igot.cb.pores.elasticsearch.dto.SearchCriteria;
import com.igot.cb.pores.elasticsearch.dto.SearchResult;
import com.igot.cb.pores.elasticsearch.service.EsUtilService;
import com.igot.cb.pores.exceptions.CustomException;
import com.igot.cb.pores.util.*;
import com.igot.cb.transactional.cassandrautils.CassandraOperation;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.client.RequestOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;
import org.springframework.web.multipart.MultipartFile;
import org.sunbird.cloud.storage.BaseStorageService;
import org.sunbird.cloud.storage.factory.StorageConfig;
import org.sunbird.cloud.storage.factory.StorageServiceFactory;
import scala.Option;

import javax.annotation.PostConstruct;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.io.InputStream;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * @author mahesh.vakkund
 */
@Service
@Slf4j
public class CommunityManagementServiceImpl implements CommunityManagementService {

    private static final String VALIDATION_FAILED_LOG = "Validation failed: {}";
    private static final String RECORD_FROM_REDIS_CACHE_LOG = "Record coming from redis cache";
    private static final String RECORD_FROM_POSTGRES_LOG = "Record coming from postgres db";
    private static final String INVALID_ID_LOG = "Invalid Id: {}";
    private static final String ERROR_JOINING_COMMUNITY_LOG = "Error while joining community:";
    private static final String ERROR_SEARCHING_LOG = "Error occured while searching:";
    private static final String FAILED_UPLOAD_FILE_LOG = "Failed to upload file. Exception: ";

    private final EsUtilService esUtilService;
    private final CacheService cacheService;
    private final ObjectMapper objectMapper;
    private final CbServerProperties cbServerProperties;
    private final PayloadValidation payloadValidation;
    private final CommunityEngagementRepository communityEngagementRepository;
    private final AccessTokenValidator accessTokenValidator;
    private final CassandraOperation cassandraOperation;
    private final RedisTemplate<String, SearchResult> redisTemplate;
    private final CommunityCategoryRepository categoryRepository;

    private Logger logger = LoggerFactory.getLogger(CommunityManagementServiceImpl.class);

    private final Producer producer;

    @Value("${kafka.topic.community.user.count}")
    private String userCountUpdateTopic;

    @Value("${no.of.popular.community}")
    private Integer noOfPopularCommunities;

    @Value("${community.category.index}")
    private String communityCategoryIndex;

    @Value("${community.index}")
    private String communityIndex;

    private BaseStorageService storageService = null;

    private final UserService userService;
    private final NotificationService notificationService;
    private final FileProcessService fileProcessService;

    @Autowired
    public CommunityManagementServiceImpl(EsUtilService esUtilService,
            CacheService cacheService,
            ObjectMapper objectMapper,
            CbServerProperties cbServerProperties,
            PayloadValidation payloadValidation,
            CommunityEngagementRepository communityEngagementRepository,
            AccessTokenValidator accessTokenValidator,
            CassandraOperation cassandraOperation,
            @Qualifier(Constants.SEARCH_RESULT_REDIS_TEMPLATE) RedisTemplate<String, SearchResult> redisTemplate,
            CommunityCategoryRepository categoryRepository,
            Producer producer,
            UserService userService,
            NotificationService notificationService,
            FileProcessService fileProcessService) {
        this.esUtilService = esUtilService;
        this.cacheService = cacheService;
        this.objectMapper = objectMapper;
        this.cbServerProperties = cbServerProperties;
        this.payloadValidation = payloadValidation;
        this.communityEngagementRepository = communityEngagementRepository;
        this.accessTokenValidator = accessTokenValidator;
        this.cassandraOperation = cassandraOperation;
        this.redisTemplate = redisTemplate;
        this.categoryRepository = categoryRepository;
        this.producer = producer;
        this.userService = userService;
        this.notificationService = notificationService;
        this.fileProcessService = fileProcessService;
    }

    @PostConstruct
    public void init() {
        if (storageService == null) {
            storageService = StorageServiceFactory.getStorageService(new StorageConfig(cbServerProperties.getCloudStorageTypeName(), cbServerProperties.getCloudStorageKey(), cbServerProperties.getCloudStorageSecret().replace("\\n", "\n"), Option.apply(cbServerProperties.getCloudStorageEndpoint()), Option.empty()));
        }
    }


    @Override
    public ApiResponse create(JsonNode communityDetails, String authToken) {
        log.info("CommunityEngagementService::create:creating community");
        ApiResponse response = ProjectUtil.createDefaultResponse(Constants.API_COMMUNITY_CREATE);
        String userId = accessTokenValidator.verifyUserToken(authToken);
        if (StringUtils.isBlank(userId)) {
            response.getParams().setStatus(Constants.FAILED);
            response.getParams().setErrMsg(Constants.USER_ID_DOESNT_EXIST);
            response.setResponseCode(HttpStatus.BAD_REQUEST);
            return response;
        }
        try {
            payloadValidation.validatePayload(Constants.PAYLOAD_VALIDATION_FILE, communityDetails);
        } catch (CustomException e) {
            log.error(VALIDATION_FAILED_LOG, e.getMessage(), e);
            response.getParams().setStatus(Constants.FAILED);
            response.getParams().setErrMsg(e.getMessage());
            response.setResponseCode(HttpStatus.BAD_REQUEST);
            return response;
        }

        try {
            // Check if the community already exists
            CommunityCategory category = categoryRepository.findByCategoryIdAndIsActive(communityDetails.get(Constants.TOPIC_ID).asInt(), true);
            if (category == null) {
                response.getParams().setStatus(Constants.FAILED);
                response.getParams().setErrMsg(Constants.TOPIC_IS_INACTIVE);
                response.setResponseCode(HttpStatus.BAD_REQUEST);
                return response;
            }
            List<Map<String, Object>> userDetails;
            Map<String, Object> propertyMap = new HashMap<>();
            propertyMap.put(Constants.ID, userId);
            userDetails = cassandraOperation.getRecordsByPropertiesWithoutFiltering(
                Constants.KEYSPACE_SUNBIRD, Constants.TABLE_USER, propertyMap,
                Arrays.asList(Constants.ROOT_ORG_ID, Constants.FIRST_NAME), 2);
            String userRootOrgId = null;
            if (!CollectionUtils.isEmpty(userDetails)) {
                userRootOrgId = (String) userDetails.get(0).get(Constants.USER_ROOT_ORG_ID);
            } else {
                log.error("User details not found in Cassandra for validating user{}", userId);
                response.getParams().setErrMsg(Constants.USER_DETAILS_NOT_FOUND);
                response.setResponseCode(HttpStatus.BAD_REQUEST);
                return response;
            }

            if (Boolean.TRUE.equals(esUtilService.doesCommunityExist(userRootOrgId,
                communityDetails.get(Constants.COMMUNITY_NAME).asText()))) {
                response.getParams().setStatus(Constants.FAILED);
                response.getParams()
                    .setErrMsg(Constants.CREATE_ERROR_MSG_WITHIN_COMMUNITY);
                response.setResponseCode(HttpStatus.CONFLICT);
                return response;
            }
            boolean isCommunityCreationAllowed = false; // Default value
            if (communityDetails.has(Constants.CommunityCreationAllowed)) {
                isCommunityCreationAllowed = communityDetails.get(Constants.CommunityCreationAllowed).asBoolean();
            }

            if (!isCommunityCreationAllowed && Boolean.TRUE.equals(esUtilService.doesCommunityNameExist(communityDetails.get(Constants.COMMUNITY_NAME).asText()))) {
                response.getParams().setStatus(Constants.FAILED);
                response.getParams().setErrMsg(
                    Constants.CREATE_ERROR_MSG_COMMUNITY);
                response.setResponseCode(HttpStatus.PRECONDITION_FAILED);
                return response;
            }
            Map<String, Object> propertyMapOrg = new HashMap<>();
            propertyMapOrg.put(Constants.ID, userRootOrgId);
            List<Map<String, Object>> orgDetails = cassandraOperation.getRecordsByPropertiesWithoutFiltering(
                Constants.KEYSPACE_SUNBIRD, Constants.ORG_TABLE, propertyMapOrg, null, 1);
            if (ObjectUtils.isEmpty(orgDetails)) {
                response.getParams().setErrMsg(Constants.ORG_DETAILS_NOT_FOUND);
                response.setResponseCode(HttpStatus.NOT_FOUND);
                return response;
            }
            if (orgDetails.get(0).containsKey(Constants.ORG_NAME)
                && orgDetails.get(0).get(Constants.ORG_NAME) != null) {
                ((ObjectNode) communityDetails).put(Constants.ORG_NAME_CAMEL_CASE,
                    orgDetails.get(0).get(Constants.ORG_NAME).toString());
            }
            String communityId = UUID.randomUUID().toString();
            CommunityEntity communityEngagementEntity = new CommunityEntity();
            communityEngagementEntity.setCommunityId(communityId);
            List<String> searchTags = new ArrayList<>();
            searchTags.add(communityDetails.get(Constants.COMMUNITY_NAME).textValue().toLowerCase());
            ArrayNode searchTagsArray = objectMapper.valueToTree(searchTags);
            ((ObjectNode) communityDetails).put(Constants.STATUS, Constants.DRAFT);
            ((ObjectNode) communityDetails).put(Constants.COMMUNITY_ID, communityId);
            ((ObjectNode) communityDetails).put(Constants.COUNT_OF_PEOPLE_JOINED, 0L);
            ((ObjectNode) communityDetails).put(Constants.COUNT_OF_PEOPLE_LIKED, 0L);
            ((ObjectNode) communityDetails).put(Constants.COUNT_OF_POST_CREATED, 0L);
            ((ObjectNode) communityDetails).put(Constants.COUNT_OF_ANSWER_POST_CREATED, 0L);
            ((ObjectNode) communityDetails).put(Constants.NO_OF_MODERATORS, 0L);
            ((ObjectNode) communityDetails).put(Constants.CREATED_BY, userId);
            ((ObjectNode) communityDetails).put(Constants.UPDATED_BY, userId);
            ((ObjectNode) communityDetails).putArray(Constants.SEARCHTAGS)
                .add(searchTagsArray);
            ((ObjectNode) communityDetails).put(Constants.ORG_ID, userRootOrgId);
            communityEngagementEntity.setData(communityDetails);
            Timestamp currentTimestamp = new Timestamp(System.currentTimeMillis());
            communityEngagementEntity.setCreatedOn(currentTimestamp);
            communityEngagementEntity.setUpdatedOn(currentTimestamp);
            communityEngagementEntity.setCreated_by(userId);
            communityEngagementEntity.setActive(true);
            CommunityEntity saveJsonEntity = communityEngagementRepository.save(communityEngagementEntity);
            if (!saveJsonEntity.getData().isNull()) {
                communityDetails = addExtraproperties(saveJsonEntity.getData(), communityId, currentTimestamp);
                Map<String, Object> communityDetailsMap = objectMapper.convertValue(communityDetails, Map.class);
                esUtilService.addDocument(communityIndex, Constants.INDEX_TYPE, communityId, communityDetailsMap, cbServerProperties.getElasticCommunityJsonPath());
                cacheService.putCache(communityId, communityDetailsMap);
                log.info(
                        "created community");
                updateCommunityCountInTopic(category, Constants.INCREMENT);
                cacheService.deleteCache(Constants.CATEGORY_LIST_ALL_REDIS_KEY_PREFIX);
                cacheService.deleteCache(generateRedisJwtTokenKey(createDefaultSearchPayload()));
                response.getResult().put(Constants.STATUS, Constants.SUCCESSFULLY_CREATED);
                response.getResult().put(Constants.COMMUNITY_ID, communityId);
                return response;

            } else {
                response.getParams().setStatus(Constants.FAILED);
                response.getParams().setErrMsg(Constants.FAILED);
                response.setResponseCode(HttpStatus.INTERNAL_SERVER_ERROR);
                return response;
            }
        } catch (Exception e) {
            log.error("error occured while creating commmunity: {}", e.getMessage(), e);
            throw new CustomException(Constants.ERROR_WHILE_PROCESSING, e.getMessage(), HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    private void updateCommunityCountInTopic(CommunityCategory category, String increment) {
        if (category != null) {
            Long currentCount = Optional.ofNullable(category.getCountOfCommunities()).orElse(0L);
            if (Constants.INCREMENT.equals(increment)) {
                category.setCountOfCommunities(currentCount + 1);
            } else {
                category.setCountOfCommunities(currentCount - 1);
            }
            Timestamp currentTimestamp = new Timestamp(System.currentTimeMillis());
            category.setLastUpdatedAt(currentTimestamp);
            categoryRepository.save(category);
            Map<String, Object> communityDetailsMap = objectMapper.convertValue(category,
                Map.class);
            communityDetailsMap.put(Constants.STATUS, Constants.ACTIVE);
            esUtilService.addDocument(communityCategoryIndex, Constants.INDEX_TYPE,
                String.valueOf(category.getCategoryId()), communityDetailsMap,
                cbServerProperties.getElasticCommunityCategoryJsonPath());
            cacheService.deleteCache(generateRedisJwtTokenKey(createDefaultSearchCriteriaForTopic()));
        }
    }

    private SearchCriteria createDefaultSearchCriteriaForTopic() {
        SearchCriteria criteria = new SearchCriteria();
        // Initialize filterCriteriaMap with default value
        HashMap<String, Object> filterMap = new HashMap<>();
        filterMap.put(Constants.STATUS, Constants.ACTIVE);
        criteria.setFilterCriteriaMap(filterMap);
        // Initialize requestedFields with default values
        List<String> fields = Arrays.asList(Constants.CATEGORY_NAME, Constants.COUNT_OF_COMMUNITIES, Constants.DEPARTMENT_ID);
        criteria.setRequestedFields(fields);
        // Set default pagination values
        criteria.setPageNumber(0);
        criteria.setPageSize(20);
        return criteria;
    }

    private SearchCriteria createDefaultSearchPayload() {
        SearchCriteria defaultCriteria = new SearchCriteria();
        HashMap<String,Object> filterCriteria = new HashMap<>();
        filterCriteria.put(Constants.STATUS, Constants.ACTIVE);
        defaultCriteria.setFilterCriteriaMap(filterCriteria);
        defaultCriteria.setFacets(Collections.singletonList((Constants.TOPIC_NAME)));
        return defaultCriteria;
    }

    private JsonNode addExtraproperties(JsonNode saveJsonEntity, String id, Timestamp currentTime) {
        log.debug("id {}", id);
        ObjectNode modifiedNode = (ObjectNode) saveJsonEntity; // Create a mutable copy of the JsonNode
        modifiedNode.put(Constants.CREATED_ON, String.valueOf(currentTime));
        modifiedNode.put(Constants.UPDATED_ON, String.valueOf(currentTime));
        return modifiedNode;
    }

    @Override
    public ApiResponse read(String communityId, String authToken) {
        log.info("CommunityEngagementService:read:reading community");
        ApiResponse response = ProjectUtil.createDefaultResponse(Constants.API_ORG_BOOKMARK_READ);
        String userId = accessTokenValidator.verifyUserToken(authToken);
        if (StringUtils.isBlank(userId)) {
            logger.error(Constants.ID_NOT_FOUND);
            response.setResponseCode(HttpStatus.INTERNAL_SERVER_ERROR);
            response.getParams().setErrMsg(Constants.ID_NOT_FOUND);
            return response;
        }
        if (StringUtils.isEmpty(communityId)) {
            logger.error(Constants.COMMUNITY_ID_NOT_FOUND);
            response.setResponseCode(HttpStatus.INTERNAL_SERVER_ERROR);
            response.getParams().setErrMsg(Constants.ID_NOT_FOUND);
            return response;
        }
        try {
            String cachedJson = cacheService.getCache(communityId);
            if (StringUtils.isNotEmpty(cachedJson)) {
                log.info(RECORD_FROM_REDIS_CACHE_LOG);
                response.getParams().setErrMsg(Constants.SUCCESSFULLY_READING);
                response
                        .getResult()
                        .put(Constants.COMMUNITY_DETAILS, objectMapper.readValue(cachedJson, new TypeReference<Object>() {
                        }));
            } else {
                Optional<CommunityEntity> communityEntityOptional = communityEngagementRepository.findByCommunityIdAndIsActive(communityId, true);
                if (communityEntityOptional.isPresent()) {
                    CommunityEntity communityEntity = communityEntityOptional.get();
                    cacheService.putCache(communityEntity.getCommunityId(),
                        communityEntityOptional.get().getData());
                    log.info(RECORD_FROM_POSTGRES_LOG);
                    response.getParams().setErrMsg(Constants.SUCCESSFULLY_READING);
                    response.getResult().put(Constants.COMMUNITY_DETAILS, objectMapper.convertValue(communityEntity.getData(), new TypeReference<Object>() {
                    }));
                } else {
                    logger.error(INVALID_ID_LOG, communityId);
                    response.setResponseCode(HttpStatus.NOT_FOUND);
                    response.getParams().setErrMsg(Constants.INVALID_COMMUNITY_ID);
                }
            }

        } catch (Exception e) {
            logger.error("Error while mapping JSON for id {}: {}", communityId, e.getMessage(), e);
            throw new CustomException(Constants.ERROR, Constants.ERROR_WHILE_PROCESSING, HttpStatus.INTERNAL_SERVER_ERROR);
        }
        return response;
    }


    public ApiResponse delete(String communityId, String authToken) {
        log.info("CommunityEngagementService:delete:deleting community");
        ApiResponse response = ProjectUtil.createDefaultResponse(Constants.API_COMMUNITY_DELETE);
        String userId = accessTokenValidator.verifyUserToken(authToken);
        if (StringUtils.isBlank(userId)) {
            response.getParams().setErrMsg(Constants.USER_ID_DOESNT_EXIST);
            response.setResponseCode(HttpStatus.BAD_REQUEST);
            return response;
        }
        if (StringUtils.isEmpty(communityId)) {
            logger.error(Constants.COMMUNITY_ID_NOT_FOUND);
            response.setResponseCode(HttpStatus.INTERNAL_SERVER_ERROR);
            response.getParams().setErrMsg(Constants.COMMUNITY_ID_NOT_FOUND);
            return response;
        }
        try {
            Optional<CommunityEntity> communityEntityOptional = communityEngagementRepository.findByCommunityIdAndIsActive(communityId, true);
            if (communityEntityOptional.isPresent()) {
                CommunityEntity communityEntity = communityEntityOptional.get();
                CommunityCategory category = categoryRepository.findByCategoryIdAndIsActive(communityEntity.getData().get(Constants.TOPIC_ID).asInt(), true);
                if (category == null) {
                    response.getParams().setStatus(Constants.FAILED);
                    response.getParams().setErrMsg(Constants.TOPIC_IS_INACTIVE);
                    response.setResponseCode(HttpStatus.BAD_REQUEST);
                    return response;
                }
                communityEntity.setActive(false);
                communityEngagementRepository.save(communityEntity);
                JsonNode esSave = communityEntity.getData();
                ((ObjectNode) esSave).put(Constants.STATUS, Constants.INACTIVE);
                Timestamp currentTimestamp = new Timestamp(System.currentTimeMillis());
                ((ObjectNode) esSave).put(Constants.UPDATED_ON, String.valueOf(currentTimestamp));
                Map<String, Object> map = objectMapper.convertValue(esSave, Map.class);
                esUtilService.updateDocument(communityIndex, Constants.INDEX_TYPE, communityId, map, cbServerProperties.getElasticCommunityJsonPath());
                updateCommunityCountInTopic(category, Constants.DECREMENT);
                cacheService.deleteCache(communityId);
                cacheService.deleteCache(Constants.CATEGORY_LIST_ALL_REDIS_KEY_PREFIX);
                cacheService.deleteCache(generateRedisJwtTokenKey(createDefaultSearchPayload()));
                response.getResult().put(Constants.RESPONSE,
                        "Deleted the community with id: " + communityId);
            } else {
                logger.error("Invalid communityId: {}", communityId);
                response.setResponseCode(HttpStatus.NOT_FOUND);
                response.getParams().setErrMsg(Constants.INVALID_COMMUNITY_ID);
            }

        } catch (Exception e) {
            logger.error("Error while deleting community {} {}", communityId, e.getMessage(), e);
            throw new CustomException(Constants.ERROR, Constants.ERROR_WHILE_PROCESSING, HttpStatus.INTERNAL_SERVER_ERROR);
        }
        return response;
    }

    @Override
    public ApiResponse update(JsonNode communityDetails, String authToken) {
        log.info("CommunityEngagementService:update:updating community");
        ApiResponse response = ProjectUtil.createDefaultResponse(Constants.API_COMMUNITY_UPDATE);
        try {
            String userId = accessTokenValidator.verifyUserToken(authToken);
            if (StringUtils.isBlank(userId)) {
                response.getParams().setErrMsg(Constants.USER_ID_DOESNT_EXIST);
                response.setResponseCode(HttpStatus.BAD_REQUEST);
                return response;
            }
            if (communityDetails.has(Constants.COMMUNITY_ID) && !communityDetails.get(Constants.COMMUNITY_ID).isNull()) {
                String communityId = communityDetails.get(Constants.COMMUNITY_ID).asText();
                Optional<CommunityEntity> communityEntityOptional = communityEngagementRepository.findByCommunityIdAndIsActive(communityId, true);
                if (!communityEntityOptional.isPresent()) {
                    response.getParams().setErrMsg(Constants.INVALID_COMMUNITY_ID);
                    response.setResponseCode(HttpStatus.BAD_REQUEST);
                    return response;
                }
                JsonNode dataNode = communityEntityOptional.get().getData();
                Iterator<Map.Entry<String, JsonNode>> fields = communityDetails.fields();
                while (fields.hasNext()) {
                    Map.Entry<String, JsonNode> field = fields.next();
                    String fieldName = field.getKey();
                    // Check if the field is present in the update JsonNode
                    if (dataNode.has(fieldName)) {
                        // Update the main JsonNode with the value from the update JsonNode
                        ((ObjectNode) dataNode).set(fieldName, communityDetails.get(fieldName));
                    } else {
                        ((ObjectNode) dataNode).put(fieldName, communityDetails.get(fieldName));
                    }
                }
                if (esUtilService.isDuplicateCommunity(dataNode.get(Constants.ORG_ID).asText(),
                    dataNode.get(Constants.COMMUNITY_NAME).asText(), dataNode.get(Constants.COMMUNITY_ID).asText())) {
                    response.getParams().setStatus(Constants.FAILED);
                    response.getParams().setErrMsg(Constants.CREATE_ERROR_MSG_WITHIN_COMMUNITY);
                    response.setResponseCode(HttpStatus.CONFLICT);
                    return response;
                }
                boolean isCommunityCreationAllowed = false; // Default value
                if (communityDetails.has(Constants.CommunityCreationAllowed)) {
                    isCommunityCreationAllowed = communityDetails.get(
                        Constants.CommunityCreationAllowed).asBoolean();
                }

                if (!isCommunityCreationAllowed && Boolean.TRUE.equals(esUtilService.doesCommunityNameExistForPublish(
                    dataNode.get(Constants.COMMUNITY_NAME).asText(),
                    dataNode.get(Constants.COMMUNITY_ID).asText()))) {
                    response.getParams().setStatus(Constants.FAILED);
                    response.getParams().setErrMsg(
                        Constants.CREATE_ERROR_MSG_COMMUNITY);
                    response.setResponseCode(HttpStatus.PRECONDITION_FAILED);
                    return response;
                }

                updateCommunityDetails(communityEntityOptional.get(),userId,dataNode, Constants.DRAFT);
                response.getResult().put(Constants.RESPONSE,
                        "Updated the community with id: " + communityId);
                cacheService.deleteCache(Constants.CATEGORY_LIST_ALL_REDIS_KEY_PREFIX);
                cacheService.deleteCache(generateRedisJwtTokenKey(createDefaultSearchPayload()));
                cacheService.deleteCache(generateRedisJwtTokenKey(createDefaultSearchCriteriaForTopic()));
                return response;

            } else {
                response.getParams().setErrMsg(Constants.COMMUNITY_ID_NOT_FOUND);
                response.setResponseCode(HttpStatus.BAD_REQUEST);
                return response;
            }


        } catch (Exception e) {
            logger.error("Error while deleting community: {}", e.getMessage(), e);
            throw new CustomException(Constants.ERROR, Constants.ERROR_WHILE_PROCESSING, HttpStatus.INTERNAL_SERVER_ERROR);

        }
    }

    @Override
    public ApiResponse joinCommunity(Map<String, Object> request, String authToken) {
        log.info("CommunityEngagementService:joinAndUnjoinCommunity:joining");
        ApiResponse response = ProjectUtil.createDefaultResponse(Constants.API_COMMUNITY_JOIN);
        try {
            String userId = accessTokenValidator.verifyUserToken(authToken);
            if (StringUtils.isBlank(userId)) {
                response.getParams().setErrMsg(Constants.USER_ID_DOESNT_EXIST);
                response.setResponseCode(HttpStatus.BAD_REQUEST);
                return response;
            }
            String error = validateJoinPayload(request);
            if (StringUtils.isNotBlank(error)) {
                return returnErrorMsg(error, HttpStatus.BAD_REQUEST, response);
            }
            String communityId = (String) request.get(Constants.COMMUNITY_ID);
            Optional<CommunityEntity> optCommunity = communityEngagementRepository.findByCommunityIdAndIsActive(
                communityId, true);
            if (!optCommunity.isPresent() || optCommunity.get().getData()
                .isEmpty()) {
                response.setResponseCode(HttpStatus.BAD_REQUEST);
                response.getParams().setErr(Constants.INVALID_COMMUNITY_ID);
                return response;
            }
            if(Constants.PRIVATE.equalsIgnoreCase(optCommunity.get().getData().path(Constants.COMMUNITY_ACCESS_LEVEL).asText())){
                response.setResponseCode(HttpStatus.BAD_REQUEST);
                response.getParams().setErr("This is a private community. Users cannot join it directly.");
                return response;
            }
            Map<String, Object> propertyMap = new HashMap<>();
            propertyMap.put(Constants.USER_ID, userId);
            propertyMap.put(Constants.COMMUNITY_ID, communityId);
            //kafka event :: es updation: upsert (postgres and es )
            List<Map<String, Object>> userCommunityDetails = cassandraOperation.getRecordsByPropertiesWithoutFiltering(
                Constants.KEYSPACE_SUNBIRD, Constants.USER_COMMUNITY_TABLE, propertyMap, null, 1);
            if (CollectionUtils.isEmpty(userCommunityDetails)) {
                Map<String, Object> parameterisedMap = new HashMap<>();
                propertyMap.put(Constants.STATUS, true);
                parameterisedMap.put(Constants.COMMUNITY_ID, communityId);
                parameterisedMap.put(Constants.USER_ID, userId);
                parameterisedMap.put(Constants.STATUS, true);
                parameterisedMap.put(Constants.LAST_UPDATED_AT,
                        Instant.now());
                cassandraOperation.insertRecord(Constants.KEYSPACE_SUNBIRD,
                    Constants.USER_COMMUNITY_TABLE, parameterisedMap);
                Map<String, Object> dataMap = new HashMap<>();
                dataMap.put(Constants.COMMUNITY, optCommunity.get());
                dataMap.put(Constants.USER_ID, userId);
                producer.push(userCountUpdateTopic, dataMap);
                esUtilService.updateUserIndex(userId,communityId,true);
                cacheService.deleteCache(generateRedisJwtTokenKey(createDefaultSearchPayload()));
                return response;
            } else {
                // Check if STATUS is false in the existing record
                Map<String, Object> existingRecord = userCommunityDetails.get(
                    0); // Fetch the first record
                Boolean status = (Boolean) existingRecord.get(Constants.STATUS);
                if (Boolean.FALSE.equals(status)) {
                    Map<String, Object> updateUserCommunityDetails = new HashMap<>();
                    Map<String, Object> updateUserCommunityLookUp = new HashMap<>();
                    updateUserCommunityDetails.put(Constants.STATUS, true);
                    updateUserCommunityDetails.put(Constants.LAST_UPDATED_AT,
                        Instant.now());
                    updateUserCommunityLookUp.put(Constants.STATUS, true);
                    cassandraOperation.updateRecord(
                        Constants.KEYSPACE_SUNBIRD, Constants.USER_COMMUNITY_TABLE,
                        updateUserCommunityDetails, propertyMap);
                    Map<String, Object> dataMap = new HashMap<>();
                    dataMap.put(Constants.COMMUNITY, optCommunity.get());
                    dataMap.put(Constants.USER_ID, userId);
                    producer.push(userCountUpdateTopic, dataMap);
                    esUtilService.updateUserIndex(userId,communityId,true);
                    cacheService.deleteCache(generateRedisJwtTokenKey(createDefaultSearchPayload()));
                    return response;

                } else {
                    // STATUS is already true - return error
                    response.setResponseCode(HttpStatus.BAD_REQUEST);
                    response.getParams().setErr(Constants.ALREADY_JOINED_COMMUNITY);
                    return response;
                }
            }
        } catch (Exception e) {
            logger.error(ERROR_JOINING_COMMUNITY_LOG, e.getMessage(), e);
            throw new CustomException(Constants.ERROR, Constants.ERROR_WHILE_PROCESSING,
                HttpStatus.INTERNAL_SERVER_ERROR);

        }
    }

    private void updateCommunityDetails(CommunityEntity communityEntity, String userId,
        JsonNode dataNode, String status) {
        Timestamp currentTime = new Timestamp(System.currentTimeMillis());
        communityEntity.setUpdatedOn(currentTime);
        ((ObjectNode) dataNode).put(Constants.UPDATED_ON, String.valueOf(currentTime));
        ((ObjectNode) dataNode).put(Constants.UPDATED_BY, userId);
        ((ObjectNode) dataNode).put(Constants.STATUS, status);
        ((ObjectNode) dataNode).put(Constants.COMMUNITY_ID, communityEntity.getCommunityId());
        if (dataNode.hasNonNull(Constants.MODERATORS) && dataNode.get(Constants.MODERATORS)
            .isArray()) {
            JsonNode moderatorsNode = dataNode.get(Constants.MODERATORS);
            long countOfModerators = moderatorsNode.size();
            ((ObjectNode) dataNode).put(Constants.NO_OF_MODERATORS, countOfModerators);
        } else {
            ((ObjectNode) dataNode).put(Constants.NO_OF_MODERATORS, 0L);
        }
        communityEntity.setData(dataNode);
        communityEngagementRepository.save(communityEntity);
        Map<String, Object> map = objectMapper.convertValue(dataNode, Map.class);
        esUtilService.updateDocument(communityIndex, Constants.INDEX_TYPE,
            communityEntity.getCommunityId(), map,
            cbServerProperties.getElasticCommunityJsonPath());
                                                                                                                                                                                        cacheService.putCache(communityEntity.getCommunityId(), communityEntity.getData());
        cacheService.deleteCache(generateRedisJwtTokenKey(createDefaultSearchCriteriaForTopic()));
    }

    @Override
    public ApiResponse communitiesJoinedByUser(String authToken) {
        log.info("CommunityEngagementService:communitiesJoinedByUser:reading");
        ApiResponse response = ProjectUtil.createDefaultResponse(Constants.API_COMMUNITY_USER_JOINED);
        try {
            String userId = accessTokenValidator.verifyUserToken(authToken);
            if (StringUtils.isBlank(userId)) {
                response.getParams().setErrMsg(Constants.USER_ID_DOESNT_EXIST);
                response.setResponseCode(HttpStatus.BAD_REQUEST);
                return response;
            }
            Map<String, Object> propertyMap = new HashMap<>();
            propertyMap.put(Constants.USER_ID, userId);
            List<String> fields = new ArrayList();
            fields.add(Constants.COMMUNITY_ID);
            fields.add(Constants.STATUS);
            List<Map<String, Object>> userCommunityDetails = cassandraOperation.getRecordsByPropertiesWithoutFiltering(
                Constants.KEYSPACE_SUNBIRD, Constants.USER_COMMUNITY_TABLE, propertyMap,
                fields, null);
            List<Object> communityEntityList = new ArrayList<>();
            if (!userCommunityDetails.isEmpty()) {
                userCommunityDetails.forEach(communityDetail -> {
                    Boolean status = (Boolean) communityDetail.get(Constants.STATUS);
                    if (Boolean.TRUE.equals(status)) {
                        String cachedJson = cacheService.getCache(
                            (String) communityDetail.get(Constants.COMMUNITY_ID_LOWERCASE));
                        if (StringUtils.isNotEmpty(cachedJson)) {
                            try {
                                communityEntityList.add(
                                    objectMapper.readValue(cachedJson,
                                        new TypeReference<Object>() {
                                        }));
                            } catch (JsonProcessingException e) {
                                logger.error(ERROR_JOINING_COMMUNITY_LOG, e.getMessage(), e);
                                throw new CustomException(Constants.ERROR, Constants.ERROR_WHILE_PROCESSING,
                                    HttpStatus.INTERNAL_SERVER_ERROR);
                            }
                        } else {
                            Optional<CommunityEntity> communityEntityOptional = communityEngagementRepository.findByCommunityIdAndIsActive(
                                (String) communityDetail.get(Constants.COMMUNITY_ID_LOWERCASE),
                                true);
                            if (communityEntityOptional.isPresent()){
                                communityEntityList.add(communityEntityOptional.get().getData());
                                cacheService.putCache(
                                    (String) communityDetail.get(Constants.COMMUNITY_ID_LOWERCASE),
                                    communityEntityOptional.get().getData());
                            }
                        }

                    }
                });
            }
            response.getResult().put(Constants.COMMUNITY_ID,
                objectMapper.convertValue(userCommunityDetails, new TypeReference<Object>() {
                }));
            response.getResult().put(Constants.COMMUNITY_DETAILS,
                objectMapper.convertValue(communityEntityList, new TypeReference<Object>() {
                }));
            return response;

        } catch (Exception e) {
            logger.error(ERROR_JOINING_COMMUNITY_LOG, e.getMessage(), e);
            throw new CustomException(Constants.ERROR, Constants.ERROR_WHILE_PROCESSING,
                HttpStatus.INTERNAL_SERVER_ERROR);

        }
    }

    @Override
    public ApiResponse listOfUsersJoined(String authToken, Map<String, Object> requestPayload) {
        log.info("CommunityEngagementService:listOfUsersJoined::reading");
        ApiResponse response = ProjectUtil.createDefaultResponse(Constants.API_COMMUNITY_LIST_USER);

        String payloadErrMsg = validatePayloadForListOfUsers(requestPayload);
        if (!payloadErrMsg.isEmpty()) {
            response.setResponseCode(HttpStatus.BAD_REQUEST);
            response.getParams().setStatus(Constants.FAILED);
            response.getParams().setErrMsg(payloadErrMsg);
            return response;
        }
        try {
            String userId = accessTokenValidator.verifyUserToken(authToken);
            if (StringUtils.isBlank(userId)) {
                response.getParams().setErrMsg(Constants.USER_ID_DOESNT_EXIST);
                response.setResponseCode(HttpStatus.BAD_REQUEST);
                return response;
            }
            String communityId = (String) requestPayload.get(Constants.COMMUNITY_ID);
            int offset = 0;
            int limit = 10;

            if (requestPayload.containsKey(Constants.OFFSET) && requestPayload.get(
                Constants.OFFSET) instanceof Number offsetNumber) {
                offset = offsetNumber.intValue();
            }

            if (requestPayload.containsKey(Constants.LIMIT) && requestPayload.get(
                Constants.LIMIT) instanceof Number limitNumber) {
                limit = limitNumber.intValue();
            }
            log.info("Fetching users from Redis for Community ID: {} with Offset: {}, Limit: {}",
                communityId, offset, limit);
            Long listSize = cacheService.getListSize(
                Constants.CMMUNITY_USER_REDIS_PREFIX + communityId);
            List<String> paginatedUserIds;
            Set<String> uniqueUserIds = new HashSet<>();
            if (listSize == null || listSize.equals(0L)) {
                paginatedUserIds = fetchDataFromPrimary(communityId, offset, limit);
                if (paginatedUserIds == null || paginatedUserIds.isEmpty()) {
                    response.getResult().put(Constants.USER_DETAILS, Collections.emptyList());
                    response.getResult().put(Constants.USER_COUNT, 0L);
                    response.setResponseCode(HttpStatus.OK);
                    return response;
                }
            }
            int startIndex = offset * limit;
            if (startIndex >= listSize) {
                response.getResult().put(Constants.USER_DETAILS, Collections.emptyList());
                response.getResult().put(Constants.USER_COUNT, 0L);
                response.setResponseCode(HttpStatus.OK);
                return response;
            }
            paginatedUserIds =
                cacheService.getPaginatedUsersFromHash(
                    Constants.CMMUNITY_USER_REDIS_PREFIX + communityId, offset, limit);

            if (paginatedUserIds == null || paginatedUserIds.isEmpty()) {
                paginatedUserIds = fetchDataFromPrimary(communityId, offset, limit);
                listSize = cacheService.getListSize(
                    Constants.CMMUNITY_USER_REDIS_PREFIX + communityId);

            }
            if (paginatedUserIds == null || paginatedUserIds.isEmpty()) {
                response.getResult().put(Constants.USER_DETAILS, Collections.emptyList());
                response.getResult().put(Constants.USER_COUNT,
                    0L);
                response.setResponseCode(HttpStatus.OK);
                return response;
            }

            // Convert Redis Objects to Strings
            for (Object userIdObj : paginatedUserIds) {
                if (userIdObj instanceof String userIdStr) {
                    uniqueUserIds.add(userIdStr);
                }
            }
            List<String> userListWithPrefix = new ArrayList<>(uniqueUserIds);
            List<Object> userList = fetchDataForKeys(userListWithPrefix);
            if (userList != null) {
                userList.replaceAll(obj -> {
                    if (obj instanceof Map<?, ?> rawMap) {
                        @SuppressWarnings("unchecked")
                        Map<String, Object> map = (Map<String, Object>) rawMap;
                        map.computeIfPresent(Constants.DESIGNATION, (k, v) ->
                            Constants.NULL_STRING.equalsIgnoreCase(String.valueOf(v)) ? "" : v);
                    }
                    return obj;
                });
            }
            Map<String, Object> userInfoList = userList.stream()
                .map(user -> (Map<String, Object>) user)
                .collect(Collectors.toMap(
                    user -> Constants.USER_PREFIX + user.get(Constants.USER_ID_KEY).toString(),
                    user -> user));
            // Remove found IDs from orgIdSet
            List<String> missingUserIds = userListWithPrefix.stream()
                .map(id -> id.replace(Constants.USER_PREFIX, ""))
                .filter(id -> !userInfoList.containsKey(Constants.USER_PREFIX + id))
                .toList();

            if (!missingUserIds.isEmpty()) {
                List<Object> cassandraResults = userService.fetchUserFromprimary(missingUserIds);
                userList.addAll(cassandraResults);
            }
            response.getResult().put(Constants.USER_DETAILS,
                objectMapper.convertValue(userList, new TypeReference<Object>() {
                }));
            response.getResult().put(Constants.USER_COUNT,
                listSize);
            response.setResponseCode(HttpStatus.OK);
            return response;
        } catch (Exception e) {
            logger.error("Error while reading list of users joined in a  community: {}",
                e.getMessage(), e);
            throw new CustomException(Constants.ERROR, Constants.ERROR_WHILE_PROCESSING,
                HttpStatus.INTERNAL_SERVER_ERROR);

        }
    }

    private List<String> fetchDataFromPrimary(String communityId, int offset, int limit) {
        log.info("No users found in Redis for Community ID: {}. Fetching from Cassandra.",
            communityId);
        Set<String> uniqueUserIds = new HashSet<>();
        List<String> paginatedUserIds = new ArrayList<>();
        Map<String, Object> propertyMap = new HashMap<>();
        propertyMap.put(Constants.COMMUNITY_ID, communityId);
        List<String> fields = new ArrayList();
        fields.add(Constants.USER_ID);
        fields.add(Constants.STATUS);
        List<Map<String, Object>> userCommunityDetails = cassandraOperation.getRecordsByPropertiesWithoutFiltering(
            Constants.KEYSPACE_SUNBIRD, Constants.USER_COMMUNITY_LOOK_UP_TABLE, propertyMap,
            fields, null);
        // Fetch from Cassandra if Redis is empty
        if (userCommunityDetails.isEmpty()) {
            return paginatedUserIds;
        }

        // Extract & store unique user IDs
        for (Map<String, Object> communityDetail : userCommunityDetails) {
            Boolean status = (Boolean) communityDetail.get(Constants.STATUS);
            if (Boolean.TRUE.equals(status)) {
                uniqueUserIds.add(
                    Constants.USER_PREFIX + communityDetail.get(Constants.USER_ID_LOWER_CASE));
            }
        }
        if (!uniqueUserIds.isEmpty()) {
            // Push data back to Redis for caching
            cacheService.addUsersToHash(Constants.CMMUNITY_USER_REDIS_PREFIX + communityId,
                uniqueUserIds);
            // Fetch paginated user IDs again from Redis
            paginatedUserIds =
                cacheService.getPaginatedUsersFromHash(
                    Constants.CMMUNITY_USER_REDIS_PREFIX + communityId, offset, limit);
            return paginatedUserIds;
        }
        return paginatedUserIds;
    }

    private String validatePayloadForListOfUsers(Map<String, Object> requestPayload) {
        List<String> errObjList = new ArrayList<>();
        if (!requestPayload.containsKey(Constants.COMMUNITY_ID) || StringUtils.isBlank(
            (String) requestPayload.get(Constants.COMMUNITY_ID))) {
            errObjList.add(Constants.COMMUNITY_ID);
        }
        if (!requestPayload.containsKey(Constants.OFFSET)
            || requestPayload.get(Constants.OFFSET) == null ||
            !(requestPayload.get(Constants.OFFSET) instanceof Integer)) {
            errObjList.add(Constants.OFFSET);
        }
        if (!requestPayload.containsKey(Constants.LIMIT)
            || requestPayload.get(Constants.LIMIT) == null ||
            !(requestPayload.get(Constants.LIMIT) instanceof Integer)) {
            errObjList.add(Constants.LIMIT);
        }
        if (!errObjList.isEmpty()) {
            return "Missing mandatory attributes or Improper dataType. " + errObjList.toString();
        }
        return "";
    }

    public List<Object> fetchDataForKeys(List<String> keys) {

        List<Object> values =
            cacheService.hget(keys);
        // Create a map of key-value pairs, converting stringified JSON objects to User objects
        return keys.stream()
            .filter(key -> values.get(keys.indexOf(key)) != null) // Filter out null values
            .map(key -> {
                String stringifiedJson = (String) values.get(keys.indexOf(key)); // Cast the value to String
                try {
                    // Convert the stringified JSON to a User object using ObjectMapper
                    return objectMapper.readValue(stringifiedJson, Object.class); // You can map this to a specific User type if needed
                } catch (Exception e) {
                    log.error("Failed to convert String to Json: String value: {}", stringifiedJson, e);
                    return null; // Return null in case of error
                }
            })
            .collect(Collectors.toList());
    }

    @Override
    public ApiResponse unJoinCommunity(Map<String, Object> request, String authToken) {
        log.info("CommunityEngagementService:unJoinCommunity::unjoining");
        ApiResponse response = ProjectUtil.createDefaultResponse(Constants.API_COMMUNITY_UNJOIN);
        try {
            String userId = accessTokenValidator.verifyUserToken(authToken);
            if (StringUtils.isBlank(userId)) {
                response.getParams().setErrMsg(Constants.USER_ID_DOESNT_EXIST);
                response.setResponseCode(HttpStatus.BAD_REQUEST);
                return response;
            }
            String error = validateJoinPayload(request);
            if (StringUtils.isNotBlank(error)) {
                return returnErrorMsg(error, HttpStatus.BAD_REQUEST, response);
            }
            String communityId = (String) request.get(Constants.COMMUNITY_ID);
            Optional<CommunityEntity> optCommunity = communityEngagementRepository.findByCommunityIdAndIsActive(
                communityId, true);
            if (!optCommunity.isPresent() || optCommunity.get().getData()
                .isEmpty()) {
                response.setResponseCode(HttpStatus.BAD_REQUEST);
                response.getParams().setErr(Constants.INVALID_COMMUNITY_ID);
                return response;
            }
            Map<String, Object> propertyMap = new HashMap<>();
            propertyMap.put(Constants.USER_ID, userId);
            propertyMap.put(Constants.COMMUNITY_ID, communityId);
            List<Map<String, Object>> userCommunityDetails = cassandraOperation.getRecordsByPropertiesWithoutFiltering(
                Constants.KEYSPACE_SUNBIRD, Constants.USER_COMMUNITY_TABLE, propertyMap, null, 1);
            if (!CollectionUtils.isEmpty(userCommunityDetails)) {
                Map<String, Object> existingRecord = userCommunityDetails.get(
                    0); // Fetch the first record
                if (Boolean.FALSE.equals(existingRecord.get(Constants.STATUS))) {
                    response.setResponseCode(HttpStatus.BAD_REQUEST);
                    response.getParams().setErr(Constants.NOT_JOINED_ALREADY);
                    return response;
                }
                Map<String, Object> updateUserCommunityDetails = new HashMap<>();
                updateUserCommunityDetails.put(Constants.STATUS, false);
                Map<String, Object> updateUserCommunityLookUp = new HashMap<>();
                updateUserCommunityDetails.put(Constants.STATUS, false);
                updateUserCommunityDetails.put(Constants.LAST_UPDATED_AT,
                    Instant.now());
                updateUserCommunityLookUp.put(Constants.STATUS, false);
                cassandraOperation.updateRecord(
                    Constants.KEYSPACE_SUNBIRD, Constants.USER_COMMUNITY_TABLE,
                    updateUserCommunityDetails, propertyMap);
                cassandraOperation.updateRecord(
                    Constants.KEYSPACE_SUNBIRD, Constants.USER_COMMUNITY_LOOK_UP_TABLE,
                    updateUserCommunityLookUp, propertyMap);
                JsonNode dataNode = optCommunity.get().getData();
                ((ObjectNode) dataNode).put(Constants.COUNT_OF_PEOPLE_JOINED,
                    dataNode.get(Constants.COUNT_OF_PEOPLE_JOINED).asInt() - 1);
                updateCommunityDetails(optCommunity.get(), userId, dataNode, Constants.ACTIVE);
                // Delete the key from Redis
                cacheService.deleteUserFromHash(Constants.CMMUNITY_USER_REDIS_PREFIX+communityId,Constants.USER_PREFIX+userId);
                esUtilService.updateUserIndex(userId,communityId,false);
                cacheService.deleteCache(generateRedisJwtTokenKey(createDefaultSearchPayload()));
                return response;
            } else {
                response.setResponseCode(HttpStatus.BAD_REQUEST);
                response.getParams().setErr(Constants.NOT_JOINED_ALREADY);
                return response;
            }
        } catch (Exception e) {
            logger.error(ERROR_JOINING_COMMUNITY_LOG, e.getMessage(), e);
            throw new CustomException(Constants.ERROR, Constants.ERROR_WHILE_PROCESSING,
                HttpStatus.INTERNAL_SERVER_ERROR);

        }
    }

    @Override
    public ApiResponse searchCommunity(SearchCriteria searchCriteria) {
        log.info("CommunityEngagementService:searchCommunity::inside method");
        ApiResponse response = ProjectUtil.createDefaultResponse(Constants.API_COMMUNITY_SEARCH);
        try {
            SearchResult searchResult;
            if (searchCriteria.isOverrideCache()) {
                return handleSearchAndCache(searchCriteria, response, communityIndex);
            }
            searchResult = redisTemplate.opsForValue()
                .get(generateRedisJwtTokenKey(searchCriteria));
            if (searchResult != null) {
                log.info(
                    "DiscussionServiceImpl::searchDiscussion:  search result fetched from redis");
                response.getResult().put(Constants.SEARCH_RESULTS, searchResult);
                createSuccessResponse(response);
                return response;
            }
            String searchString = searchCriteria.getSearchString();
            if (searchString != null && searchString.length() < 2) {
                createErrorResponse(response, Constants.MINIMUM_CHARACTERS_NEEDED,
                    HttpStatus.BAD_REQUEST, Constants.FAILED_CONST);
                return response;
            }
            return handleSearchAndCache(searchCriteria, response, communityIndex);
        } catch (Exception e) {
            logger.error(ERROR_SEARCHING_LOG, e);
            throw new CustomException(Constants.ERROR, Constants.ERROR_WHILE_PROCESSING,
                HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    @Override
    public ApiResponse categoryCreate(JsonNode categoryDetails, String authToken) {
        log.info("CommunityEngagementService:categoryCreate:creating");
        ApiResponse response = ProjectUtil.createDefaultResponse(Constants.API_CATEGORY_CRAETE);
        String userId = accessTokenValidator.verifyUserToken(authToken);
        if (StringUtils.isBlank(userId)) {
            response.getParams().setStatus(Constants.FAILED);
            response.getParams().setErrMsg(Constants.USER_ID_DOESNT_EXIST);
            response.setResponseCode(HttpStatus.BAD_REQUEST);
            return response;
        }
        List<Map<String, Object>> userDetails;
        Map<String, Object> propertyMap = new HashMap<>();
        propertyMap.put(Constants.ID, userId);
        userDetails = cassandraOperation.getRecordsByPropertiesWithoutFiltering(
            Constants.KEYSPACE_SUNBIRD, Constants.TABLE_USER, propertyMap,
            Arrays.asList(Constants.ROOT_ORG_ID, Constants.FIRST_NAME), 2);
        String userRootOrgId = (!CollectionUtils.isEmpty(userDetails) &&
            userDetails.get(0).get(Constants.USER_ROOT_ORG_ID) != null)
            ? (String) userDetails.get(0).get(Constants.USER_ROOT_ORG_ID)
            : null;
        if (Objects.isNull(userRootOrgId)) {
            log.error("User Root Org ID is missing or null for user{}", userId);
            response.getParams().setErrMsg(Constants.USER_DETAILS_NOT_FOUND);
            response.setResponseCode(HttpStatus.BAD_REQUEST);
            return response;
        }

        try {
            payloadValidation.validatePayload(Constants.CATEGORY_PAYLOAD_VALIDATION_FILE, categoryDetails);
        } catch (CustomException e) {
            log.error(VALIDATION_FAILED_LOG, e.getMessage(), e);
            response.getParams().setStatus(Constants.FAILED);
            response.getParams().setErrMsg(e.getMessage());
            response.setResponseCode(HttpStatus.BAD_REQUEST);
            return response;
        }
        Timestamp currentTimestamp = new Timestamp(System.currentTimeMillis());
        try {
            if (categoryDetails.has(Constants.PARENT_ID)) {
                Optional<CommunityCategory> communityCatgoryOptional = Optional.ofNullable(
                    categoryRepository.findByParentIdAndCategoryNameAndDepartmentIdAndIsActive(
                        categoryDetails.get(Constants.PARENT_ID).asInt(),
                        categoryDetails.get(Constants.CATEGORY_NAME).asText(),
                        categoryDetails.get(Constants.DEPARTMENT_ID).asText(), true));
                if (communityCatgoryOptional.isPresent()) {
                    response.getParams().setStatus(Constants.FAILED);
                    response.getParams()
                        .setErrMsg(Constants.ALREADY_PRESENT_COMMUNITY_UNDER_THIS_TOPIC);
                    response.setResponseCode(HttpStatus.BAD_REQUEST);
                    return response;
                }
                CommunityCategory communityCategorySaved = persistCategoryInPrimary(categoryDetails,
                    categoryDetails.get(Constants.PARENT_ID).asInt(), userId, currentTimestamp, userRootOrgId);

                Map<String, Object> communityDetailsMap = objectMapper.convertValue(categoryDetails,
                    Map.class);
                esUtilService.updateDocument(communityCategoryIndex, Constants.INDEX_TYPE,
                    String.valueOf(communityCategorySaved.getCategoryId()), communityDetailsMap,
                    cbServerProperties.getElasticCommunityCategoryJsonPath());
                response.getResult().put(Constants.STATUS, Constants.SUCCESSFULLY_CREATED);
                response.getResult()
                    .put(Constants.CATEGORY_ID, communityCategorySaved.getCategoryId());
                cacheService.deleteCache(generateRedisJwtTokenKey(createDefaultSearchCriteriaForTopic()));
                return response;

            } else {
                Optional<CommunityCategory> communityCatgoryOptional = Optional.ofNullable(
                    categoryRepository.findByCategoryNameAndIsActive(
                        categoryDetails.get(Constants.CATEGORY_NAME).asText(), true)
                );
                if (communityCatgoryOptional.isPresent()) {
                    response.getParams().setStatus(Constants.FAILED);
                    response.getParams().setErrMsg(Constants.ALREADY_CATEGORY_PRESENT);
                    response.setResponseCode(HttpStatus.BAD_REQUEST);
                    return response;
                }
                CommunityCategory savedCategory = persistCategoryInPrimary(categoryDetails, 0,
                    userId, currentTimestamp, userRootOrgId);
                Map<String, Object> communityDetailsMap = objectMapper.convertValue(savedCategory,
                    Map.class);
                communityDetailsMap.put(Constants.CATEGORY_ID, savedCategory.getCategoryId());
                communityDetailsMap.put(Constants.STATUS, Constants.ACTIVE);
                esUtilService.addDocument(communityCategoryIndex, Constants.INDEX_TYPE,
                    String.valueOf(savedCategory.getCategoryId()), communityDetailsMap,
                    cbServerProperties.getElasticCommunityCategoryJsonPath());
                response.getResult().put(Constants.STATUS, Constants.SUCCESSFULLY_CREATED);
                response.getResult().put(Constants.CATEGORY_ID, savedCategory.getCategoryId());
                cacheService.deleteCache(generateRedisJwtTokenKey(createDefaultSearchCriteriaForTopic()));
                return response;

            }
        } catch (Exception e) {
            log.error("error occured while creating category: {}", e.getMessage(), e);
            throw new CustomException(Constants.ERROR_WHILE_PROCESSING, e.getMessage(),
                HttpStatus.INTERNAL_SERVER_ERROR);
        }

    }

    @Override
    public ApiResponse readCategory(String categoryId, String authToken) {
        log.info("CommunityEngagementService:readCategory:reading community");
        ApiResponse response = ProjectUtil.createDefaultResponse(Constants.API_CATEGORY_READ);
        String userId = accessTokenValidator.verifyUserToken(authToken);
        if (StringUtils.isBlank(userId)) {
            logger.error(Constants.ID_NOT_FOUND);
            response.setResponseCode(HttpStatus.INTERNAL_SERVER_ERROR);
            response.getParams().setErrMsg(Constants.ID_NOT_FOUND);
            return response;
        }
        if (StringUtils.isEmpty(categoryId)) {
            logger.error("categoryId not found");
            response.setResponseCode(HttpStatus.INTERNAL_SERVER_ERROR);
            response.getParams().setErrMsg(Constants.ID_NOT_FOUND);
            return response;
        }
        try {
            Optional<CommunityCategory> categoryOptional = Optional.ofNullable(
                categoryRepository.findByCategoryIdAndIsActive(
                    Integer.valueOf(categoryId), true));
            if (categoryOptional.isPresent()) {
                CommunityCategory category = categoryOptional.get();
                log.info(RECORD_FROM_POSTGRES_LOG);
                response.getParams().setErrMsg(Constants.SUCCESSFULLY_READING);
                response.getResult().put(Constants.COMMUNITY_DETAILS,
                    objectMapper.convertValue(category, new TypeReference<Object>() {
                    }));
                return response;
            } else {
                logger.error(INVALID_ID_LOG, categoryId);
                response.setResponseCode(HttpStatus.NOT_FOUND);
                response.getParams().setErrMsg(Constants.INVALID_CATEGORY_ID);
                return response;
            }

        } catch (Exception e) {
            logger.error("Error while reading category {}: {}", categoryId, e.getMessage(), e);
            throw new CustomException(Constants.ERROR, Constants.ERROR_WHILE_PROCESSING,
                HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    @Override
    public ApiResponse deleteCategory(String categoryId, String authToken) {
        log.info("CommunityEngagementService:deleteCategory:deleting community");
        ApiResponse response = ProjectUtil.createDefaultResponse(Constants.API_CATEGORY_DELETE);
        String userId = accessTokenValidator.verifyUserToken(authToken);
        if (StringUtils.isBlank(userId)) {
            logger.error(Constants.ID_NOT_FOUND);
            response.setResponseCode(HttpStatus.INTERNAL_SERVER_ERROR);
            response.getParams().setErrMsg(Constants.ID_NOT_FOUND);
            return response;
        }
        if (StringUtils.isEmpty(categoryId)) {
            logger.error("categoryId not found");
            response.setResponseCode(HttpStatus.INTERNAL_SERVER_ERROR);
            response.getParams().setErrMsg(Constants.ID_NOT_FOUND);
            return response;
        }
        try {
            Optional<CommunityCategory> categoryOptional = Optional.ofNullable(
                categoryRepository.findByCategoryIdAndIsActive(
                    Integer.valueOf(categoryId), true));
            if (categoryOptional.isPresent()) {
                CommunityCategory communityCategory = categoryOptional.get();
                Timestamp currentTimestamp = new Timestamp(System.currentTimeMillis());
                communityCategory.setIsActive(false);
                communityCategory.setLastUpdatedAt(currentTimestamp);
                categoryRepository.save(communityCategory);
                JsonNode esSave = objectMapper.valueToTree(communityCategory);
                ((ObjectNode) esSave).put(Constants.STATUS, Constants.INACTIVE);
                ((ObjectNode) esSave).put(Constants.UPDATED_ON, String.valueOf(currentTimestamp));
                Map<String, Object> map = objectMapper.convertValue(esSave, Map.class);
                esUtilService.updateDocument(communityCategoryIndex, Constants.INDEX_TYPE,
                    categoryId, map, cbServerProperties.getElasticCommunityCategoryJsonPath());
                response.getResult().put(Constants.RESPONSE,
                    "Deleted the category with id: " + categoryId);
                cacheService.deleteCache(generateRedisJwtTokenKey(createDefaultSearchCriteriaForTopic()));
                return response;

            } else {
                logger.error("Invalid categoryId: {}", categoryId);
                response.setResponseCode(HttpStatus.NOT_FOUND);
                response.getParams().setErrMsg(Constants.INVALID_CATEGORY_ID);
                return response;
            }

        } catch (Exception e) {
            logger.error("Error while deleting category {}: {}", categoryId, e.getMessage(), e);
            throw new CustomException(Constants.ERROR, Constants.ERROR_WHILE_PROCESSING,
                HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }



    @Override
    public ApiResponse updateCategory(JsonNode categoryDetails, String authToken) {
        log.info("CommunityEngagementService:updateCategory:updating category");
        ApiResponse response = ProjectUtil.createDefaultResponse(Constants.API_CATEGORY_UPDATE);
        String userId = accessTokenValidator.verifyUserToken(authToken);
        if (StringUtils.isBlank(userId)) {
            logger.error(Constants.ID_NOT_FOUND);
            response.setResponseCode(HttpStatus.INTERNAL_SERVER_ERROR);
            response.getParams().setErrMsg(Constants.ID_NOT_FOUND);
            return response;
        }
        try {
            payloadValidation.validatePayload(Constants.CATEGORY_PAYLOAD_VALIDATION_FILE, categoryDetails);
        } catch (CustomException e) {
            log.error(VALIDATION_FAILED_LOG, e.getMessage(), e);
            response.getParams().setStatus(Constants.FAILED);
            response.getParams().setErrMsg(e.getMessage());
            response.setResponseCode(HttpStatus.BAD_REQUEST);
            return response;
        }
        try {
            if (categoryDetails.has(Constants.CATEGORY_ID) && !categoryDetails.get(
                Constants.CATEGORY_ID).isNull()) {
                Optional<CommunityCategory> categoryOptional = Optional.ofNullable(
                    categoryRepository.findByCategoryIdAndIsActive(
                        categoryDetails.get(Constants.CATEGORY_ID).asInt(), true));
                if (!categoryOptional.isPresent()) {
                    response.getParams().setErrMsg(Constants.INVALID_CATEGORY_ID);
                    response.setResponseCode(HttpStatus.BAD_REQUEST);
                    return response;
                }
                CommunityCategory communityCategory = objectMapper.convertValue(categoryDetails,
                    CommunityCategory.class);
                Timestamp currentTimestamp = new Timestamp(System.currentTimeMillis());
                communityCategory.setIsActive(true);
                communityCategory.setLastUpdatedAt(currentTimestamp);
                communityCategory.setCreatedAt(categoryOptional.get().getCreatedAt());
                categoryRepository.save(communityCategory);
                JsonNode esSave = objectMapper.valueToTree(communityCategory);
                ((ObjectNode) esSave).put(Constants.STATUS, Constants.ACTIVE);
                ((ObjectNode) esSave).put(Constants.UPDATED_ON, String.valueOf(currentTimestamp));
                Map<String, Object> map = objectMapper.convertValue(esSave, Map.class);
                esUtilService.updateDocument(communityCategoryIndex, Constants.INDEX_TYPE,
                    String.valueOf(categoryDetails.get(Constants.CATEGORY_ID)), map,
                    cbServerProperties.getElasticCommunityCategoryJsonPath());
                response.getResult().put(Constants.RESPONSE,
                    "Updated the category with id: " + categoryDetails.get(Constants.CATEGORY_ID));
                cacheService.deleteCache(generateRedisJwtTokenKey(createDefaultSearchCriteriaForTopic()));
                return response;
            } else {
                response.getParams().setErrMsg(Constants.COMMUNITY_ID_NOT_FOUND);
                response.setResponseCode(HttpStatus.BAD_REQUEST);
                return response;
            }

        } catch (Exception e) {
            logger.error("Error while updating category {}: {}",
                categoryDetails.has(Constants.CATEGORY_ID), e.getMessage(), e);
            throw new CustomException(Constants.ERROR, Constants.ERROR_WHILE_PROCESSING,
                HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    @Override
    public ApiResponse listOfCategory() {
        log.info("CommunityEngagementService:listOfCategory:listing");
        ApiResponse response = ProjectUtil.createDefaultResponse(Constants.API_CATEGORY_LIST);
        try {
            String cachedJson = cacheService.getCache(Constants.CATEGORY_LIST_REDIS_KEY_PREFIX);
            if (StringUtils.isNotEmpty(cachedJson)) {
                log.info(RECORD_FROM_REDIS_CACHE_LOG);
                response.getParams().setErrMsg(Constants.SUCCESSFULLY_READING);
                response
                    .getResult()
                    .put(Constants.CATEGORY_DETAILS,
                        objectMapper.readValue(cachedJson, new TypeReference<Object>() {
                        }));
                return response;
            }
            List<CommunityCategory> optListCategories = categoryRepository.findByParentIdAndIsActive(
                0, true);
            if (optListCategories.isEmpty()) {
                response.getParams().setErrMsg(Constants.CATEGORIES_NOT_FOUND);
                response.setResponseCode(HttpStatus.BAD_REQUEST);
                return response;
            }
            // Convert the entire list to a JSON-compatible structure and set it in the response
            List<Object> categoryDetailsList = objectMapper.convertValue(optListCategories,
                new TypeReference<List<Object>>() {
                });
            response.getResult().put(Constants.CATEGORY_DETAILS, categoryDetailsList);
            cacheService.putCache(Constants.CATEGORY_LIST_REDIS_KEY_PREFIX, categoryDetailsList);
            return response;
        } catch (Exception e) {
            logger.error("Error while listing the categories: {}"
                , e.getMessage(), e);
            throw new CustomException(Constants.ERROR, Constants.ERROR_WHILE_PROCESSING,
                HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    @Override
    public ApiResponse listOfSubCategory(SearchCriteria searchCriteria) {
        log.info("CommunityEngagementService:listOfSubCategory:listing");
        ApiResponse response = ProjectUtil.createDefaultResponse(Constants.API_SUB_CATEGORY_LIST);
        try {
            // Check if filterCriteriaMap exists and contains the categoryId key with a non-null value
            if (searchCriteria != null
                && searchCriteria.getFilterCriteriaMap() != null
                && searchCriteria.getFilterCriteriaMap().containsKey(Constants.CATEGORY_ID)
                && searchCriteria.getFilterCriteriaMap().get(Constants.CATEGORY_ID) != null) {

                Optional<CommunityCategory> categoryOptional = Optional.ofNullable(
                    categoryRepository.findByCategoryIdAndIsActive(
                        (Integer) searchCriteria.getFilterCriteriaMap().get(Constants.CATEGORY_ID),
                        true));
                if (!categoryOptional.isPresent()) {
                    response.getParams().setErrMsg(Constants.INVALID_CATEGORY_ID);
                    response.setResponseCode(HttpStatus.BAD_REQUEST);
                    return response;
                }
                searchCriteria.getFilterCriteriaMap().put(Constants.PARENT_ID,
                    searchCriteria.getFilterCriteriaMap().get(Constants.CATEGORY_ID));
                searchCriteria.getFilterCriteriaMap().put(Constants.STATUS, Constants.ACTIVE);
                // Remove CATEGORY_ID from the map
                searchCriteria.getFilterCriteriaMap().remove(Constants.CATEGORY_ID);
                SearchResult searchResult = redisTemplate.opsForValue()
                    .get(generateRedisJwtTokenKey(searchCriteria));
                if (searchResult != null) {
                    log.info(
                        "CommunityEngagementService::listOfSubCategory:  search result fetched from redis");
                    response.getResult().put(Constants.CATEGORY_DETAILS,
                        objectMapper.convertValue(categoryOptional.get(),
                            new TypeReference<Map<String, Object>>() {
                            }));
                    createSuccessResponse(response);
                    response.getResult().put(Constants.SUB_CATEGORIES, searchResult);
                    createSuccessResponse(response);
                    return response;
                }
                String searchString = searchCriteria.getSearchString();
                if (searchString != null && searchString.length() < 2) {
                    createErrorResponse(response, Constants.MINIMUM_CHARACTERS_NEEDED,
                        HttpStatus.BAD_REQUEST, Constants.FAILED_CONST);
                    return response;
                }
                searchResult = esUtilService.searchDocuments(communityCategoryIndex,
                    searchCriteria);
                redisTemplate.opsForValue().set(
                    generateRedisJwtTokenKey(searchCriteria),
                    searchResult,
                    cbServerProperties.getSearchResultRedisTtl(),
                    TimeUnit.SECONDS
                );
                response.getResult().put(Constants.CATEGORY_DETAILS,
                    objectMapper.convertValue(categoryOptional.get(),
                        new TypeReference<Map<String, Object>>() {
                        }));
                createSuccessResponse(response);
                response.getResult().put(Constants.SUB_CATEGORIES, searchResult);
                return response;

            } else {
                response.getParams().setErrMsg(Constants.INVALID_CATEGORY_ID);
                response.setResponseCode(HttpStatus.BAD_REQUEST);
                return response;
            }


        } catch (Exception e) {
            logger.error("Error while listing the sub-categories: {}"
                , e.getMessage(), e);
            throw new CustomException(Constants.ERROR, Constants.ERROR_WHILE_PROCESSING,
                HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    @Override
    public ApiResponse lisAllCategoryWithSubCat() {
        log.info("CommunityEngagementService:lisAllCategoryWithSubCat:listing");
        ApiResponse response = ProjectUtil.createDefaultResponse(
            Constants.API_SUB_CATEGORY_LIST_ALL);
        try {
            String cachedJson = cacheService.getCache(Constants.CATEGORY_LIST_ALL_REDIS_KEY_PREFIX);
            if (StringUtils.isNotEmpty(cachedJson)) {
                log.info(RECORD_FROM_REDIS_CACHE_LOG);
                Map<String, Object> cachedData;

                cachedData = objectMapper.readValue(cachedJson,
                    new TypeReference<Map<String, Object>>() {
                    });

                response.getParams().setErrMsg(Constants.SUCCESSFULLY_READING);
                response
                    .setResult(cachedData);
                return response;
            }
            List<CommunityCategory> optListCategories = categoryRepository.findByParentIdAndIsActive(
                0, true);
            if (optListCategories.isEmpty()) {
                response.getParams().setErrMsg(Constants.CATEGORIES_NOT_FOUND);
                response.setResponseCode(HttpStatus.NOT_FOUND);
            }
            List<Integer> topicIds = optListCategories.stream()
                .map(CommunityCategory::getCategoryId) // Assuming getId() retrieves the ID
                .toList();
            SearchResult searchResult
                = esUtilService.fetchTopCommunitiesForTopics(topicIds, communityIndex);
            if (!searchResult.getData().isEmpty()) {
                List<Map<String, Object>> documents;
                documents = objectMapper.convertValue(
                    searchResult.getData(),
                    new TypeReference<List<Map<String, Object>>>() {
                    }
                );
                Set<String> uniqueOrgIds = new HashSet<>();
                // Extract 'data' field from searchResult
                JsonNode dataNode = searchResult.getData();
                if (dataNode != null && dataNode.isArray()) {
                    for (JsonNode item : dataNode) {
                        if (item.has(Constants.ORD_ID) && !item.get(Constants.ORD_ID).isNull()) {
                            JsonNode orgIdNode = item.get(Constants.ORD_ID);
                            if (orgIdNode.isTextual()) {
                                uniqueOrgIds.add(orgIdNode.asText());
                            }
                        }
                    }
                }
                // Convert Set to List
                List<String> orgIdList = new ArrayList<>(uniqueOrgIds);
                Map<String, Object> propertyMap = new HashMap<>();
                propertyMap.put(Constants.ID, orgIdList);
                List<Map<String, Object>> orgInfoList = cassandraOperation.getRecordsByPropertiesWithoutFiltering(
                    Constants.KEYSPACE_SUNBIRD, Constants.TABLE_ORGANISATION, propertyMap,
                    Arrays.asList(Constants.LOGO, Constants.ORG_NAME, Constants.ID), null);
                // Create a result map to hold all categories
                Map<String, Object> result = new HashMap<>();
                result.put(Constants.FACETS, searchResult.getFacets());
                result.put(Constants.ORG_LIST, orgInfoList);

// List to store all parent categories
                List<Map<String, Object>> categoryList = new ArrayList<>();

// Process each parent category
                optListCategories.forEach(parentCategory -> {
                    // Filter subcategories related to the current parent category
                    List<Map<String, Object>> subCategories = documents.stream()
                        .filter(doc -> doc.get(Constants.TOPIC_ID) != null &&
                            doc.get(Constants.TOPIC_ID).equals(parentCategory.getCategoryId()))
                        .toList();

                    // Build the parent category map
                    Map<String, Object> parentCategoryMap = new HashMap<>();
                    parentCategoryMap.put(Constants.TOPIC_ID, parentCategory.getCategoryId());
                    parentCategoryMap.put(Constants.TOPIC_NAME, parentCategory.getCategoryName());
                    parentCategoryMap.put(Constants.COMMUNITIES, subCategories);

                    // Add this category to the list
                    categoryList.add(parentCategoryMap);
                });

// Store the list in the result map
                result.put(Constants.DATA, categoryList);

                // Set the result in the response
                response.setResponseCode(HttpStatus.OK);
                response.setResult(result);
                return response;
            } else {
                response.getParams().setErrMsg(Constants.CATEGORIES_NOT_FOUND);
                response.setResponseCode(HttpStatus.NOT_FOUND);
                return response;
            }


        } catch (Exception e) {
            logger.error("Error while listing all categoires with subCategories: {}"
                , e.getMessage(), e);
            throw new CustomException(Constants.ERROR, Constants.ERROR_WHILE_PROCESSING,
                HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    @Override
    public ApiResponse getPopularCommunitiesByField(Map<String, Object> payload) {
        log.info("CommunityEngagementService:getPopularCommunitiesByField:listing");
        ApiResponse response = ProjectUtil.createDefaultResponse(
            Constants.API_POPULAR_COMMUNITY);
        try {
            if (payload.isEmpty() || !payload.containsKey(Constants.FIELD)
                || payload.get(Constants.FIELD) == null) {
                response.getParams().setStatus(Constants.FAILED);
                response.getParams().setErrMsg("Field is mandatory");
                response.setResponseCode(HttpStatus.BAD_REQUEST);
                return response;

            }
            int offset = 0;
            int limit = noOfPopularCommunities;

            if (payload.containsKey(Constants.OFFSET) && payload.get(
                Constants.OFFSET) instanceof Number offsetNumber) {
                offset = offsetNumber.intValue();
            }

            if (payload.containsKey(Constants.LIMIT) && payload.get(
                Constants.LIMIT) instanceof Number limitNumber) {
                limit = limitNumber.intValue();
            }
            // Build the query
            Query query = Query.of(q -> q.bool(b -> b
                .filter(f -> f.term(t -> t.field(Constants.STATUS).value(Constants.ACTIVE)))
            ));

// Build the sort options
            SortOptions sortOptions = SortOptions.of(s -> s
                .field(f -> f
                    .field((String) payload.get(Constants.FIELD))
                    .order(SortOrder.Desc)
                )
            );

// Build the aggregation
            Aggregation aggregation = Aggregation.of(a -> a
                .terms(t -> t
                    .field((String) payload.get(Constants.FIELD))
                    .size(10)
                )
            );

// Build the search request
            SearchRequest searchRequest = new SearchRequest.Builder()
                .index(communityIndex)
                .query(query)
                .sort(sortOptions)
                .size(limit)
                .from(offset)
                .aggregations(payload.get(Constants.FIELD) + "_terms", aggregation)
                .build();

// Execute the search request
            // Execute the search request
            SearchResponse<Map<String, Object>> searchResponse = esUtilService.popularCommunities(searchRequest,
                RequestOptions.DEFAULT);

// Process search hits
            List<Map<String, Object>> documents = searchResponse.hits().hits().stream()
                .filter(hit -> hit.source() != null)
                .map(Hit::source)
                .toList();
            response.getResult().put(Constants.DATA, documents);

// Process aggregations
            if (searchResponse.aggregations() != null) {
                String aggregationField = payload.get(Constants.FIELD) + "_terms";
                if (searchResponse.aggregations().containsKey(aggregationField)) {
                    Aggregate aggregate = searchResponse.aggregations().get(aggregationField);

                    if (aggregate != null && aggregate.isSterms()) {
                        StringTermsAggregate termsAgg = aggregate.sterms();
                        List<Map<String, Object>> buckets = termsAgg.buckets().array().stream()
                            .map(bucket -> {
                                Map<String, Object> bucketMap = new HashMap<>();
                                bucketMap.put("key", bucket.key()); // already a String
                                bucketMap.put("doc_count", bucket.docCount());
                                return bucketMap;
                            })
                            .toList();

                        response.getResult().put(Constants.FACETS, buckets);
                    }

                }
            }

            return response;

        } catch (Exception e) {
            logger.error("Error while executing Elasticsearch query: {}", e.getMessage(), e);
            throw new CustomException("Error while processing", e.getMessage(),
                HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    @Override
    public ApiResponse report(String token, Map<String, Object> reportData) {
        log.info("CommunityService::report: Reporting community");
        ApiResponse response = ProjectUtil.createDefaultResponse("community.report");
        String errorMsg = validateReportPayload(reportData);
        if (StringUtils.isNotEmpty(errorMsg)) {
            return returnErrorMsg(errorMsg, HttpStatus.BAD_REQUEST, response);
        }

        String userId = accessTokenValidator.verifyUserToken(token);
        if (StringUtils.isBlank(userId) || Constants.UNAUTHORIZED.equals(userId)) {
            return returnErrorMsg(Constants.INVALID_AUTH_TOKEN, HttpStatus.UNAUTHORIZED, response);
        }

        try {
            String communityId = (String) reportData.get(Constants.COMMUNITY_ID);
            Optional<CommunityEntity> communityData = communityEngagementRepository.findById(
                communityId);
            if (!communityData.isPresent()) {
                return returnErrorMsg(Constants.COMMUNITY_NOT_FOUND, HttpStatus.NOT_FOUND,
                    response);
            }

            CommunityEntity communityEntity = communityData.get();
            if (!communityEntity.isActive()) {
                return returnErrorMsg(Constants.COMMUNITY_IS_INACTIVE, HttpStatus.CONFLICT,
                    response);
            }
            ObjectNode data = (ObjectNode) communityEntity.getData();
            if (data.has(Constants.STATUS) && data.get(Constants.STATUS).asText()
                .equals(Constants.SUSPENDED)) {
                return returnErrorMsg(Constants.COMMUNITY_SUSPENDED, HttpStatus.CONFLICT, response);
            }

            // Check if the user has already reported the discussion
            Map<String, Object> reportCheckData = new HashMap<>();
            reportCheckData.put(Constants.USER_ID_LOWER_CASE, userId);
            reportCheckData.put(Constants.COMMUNITY_ID_LOWERCASE, communityId);
            List<Map<String, Object>> existingReports = cassandraOperation.getRecordsByPropertiesWithoutFiltering(
                Constants.KEYSPACE_SUNBIRD, Constants.USER_REPORTED_COMMUNITY, reportCheckData,
                null, null);

            if (!existingReports.isEmpty()) {
                return returnErrorMsg("User has already reported this community",
                    HttpStatus.CONFLICT, response);
            }

            // Store user data in Cassandra
            Map<String, Object> userReportData = new HashMap<>();
            userReportData.put(Constants.USER_ID_LOWER_CASE, userId);
            userReportData.put(Constants.COMMUNITY_ID_LOWERCASE, communityId);
            if (reportData.containsKey(Constants.REPORTED_REASON)) {
                List<String> reportedReasonList = (List<String>) reportData.get(
                    Constants.REPORTED_REASON);
                if (reportedReasonList != null && !reportedReasonList.isEmpty()) {
                    StringBuilder reasonBuilder = new StringBuilder(
                        String.join(", ", reportedReasonList));

                    if (reportedReasonList.contains(Constants.OTHERS) && reportData.containsKey(
                        Constants.OTHER_REASON)) {
                        reasonBuilder.append(", ").append(reportData.get(Constants.OTHER_REASON));
                    }
                    userReportData.put(Constants.REASON, reasonBuilder.toString());
                }
            }
            userReportData.put(Constants.CREATED_ON, new Timestamp(System.currentTimeMillis()));
            cassandraOperation.insertRecord(Constants.KEYSPACE_SUNBIRD,
                Constants.USER_REPORTED_COMMUNITY, userReportData);
            cassandraOperation.insertRecord(Constants.KEYSPACE_SUNBIRD,
                Constants.COMMUNITY_REPORTED_BY_USER, userReportData);

            // Update the status of the discussion in Cassandra
            List<Map<String, Object>> reportedByUsers = cassandraOperation.getRecordsByPropertiesWithoutFiltering(
                Constants.KEYSPACE_SUNBIRD, Constants.COMMUNITY_REPORTED_BY_USER,
                Collections.singletonMap(Constants.COMMUNITY_ID_LOWERCASE, communityId), null,
                null);

            int reportCount = reportedByUsers.size();
            String status =
                reportCount >= cbServerProperties.getReporCommunityUserLimit() ? Constants.SUSPENDED
                    : Constants.REPORTED;

            ObjectNode jsonNode = objectMapper.createObjectNode();

            if (!data.get(Constants.STATUS).textValue().equals(status)) {
                data.put(Constants.STATUS, status);
            }
            if (data.has(Constants.REPORTED_BY)) {
                JsonNode reportedByNode = data.get(Constants.REPORTED_BY);
                ArrayNode reportedByArray;

                if (reportedByNode.isArray()) {
                    // 'reportedBy' is already an array
                    reportedByArray = (ArrayNode) reportedByNode;
                } else {
                    // 'reportedBy' is a single value, convert it to an array
                    reportedByArray = objectMapper.createArrayNode();
                    reportedByArray.add(reportedByNode);
                }

                // Append the new 'userId' to the array
                reportedByArray.add(userId);
                data.set(Constants.REPORTED_BY, reportedByArray);
            } else {
                // 'reportedBy' does not exist, create a new array with 'userId'
                ArrayNode reportedByArray = objectMapper.createArrayNode();
                reportedByArray.add(userId);
                data.set(Constants.REPORTED_BY, reportedByArray);
            }
            communityEngagementRepository.save(communityEntity);
            jsonNode.setAll(data);
            Map<String, Object> map = objectMapper.convertValue(jsonNode, Map.class);
            esUtilService.updateDocument(communityIndex, Constants.INDEX_TYPE, communityId,
                map, cbServerProperties.getElasticCommunityJsonPath());
            cacheService.putCache(Constants.REDIS_KEY_PREFIX + communityId, jsonNode);
            map.put(Constants.COMMUNITY_ID, reportData.get(Constants.COMMUNITY_ID));
            response.setResult(map);
            return response;
        } catch (Exception e) {
            log.error("CommunityService::report: Failed to report community", e);
            return returnErrorMsg(Constants.COMMUNITY_REPORT_FAILED,
                HttpStatus.INTERNAL_SERVER_ERROR, response);
        }
    }

    @Override
    public ApiResponse uploadFile(MultipartFile mFile, String communityId) {
        ApiResponse response = ProjectUtil.createDefaultResponse(Constants.COMMUNITY_UPLOAD_FILE);
        if (mFile.isEmpty()) {
            return returnErrorMsg(Constants.COMMUNITY_FILE_EMPTY, HttpStatus.BAD_REQUEST, response);
        }
        if (StringUtils.isBlank(communityId)) {
            return returnErrorMsg(Constants.INVALID_COMMUNITY_ID, HttpStatus.BAD_REQUEST, response);
        }

        File file = null;
        try {
            file = new File(System.currentTimeMillis() + "_" + mFile.getOriginalFilename());

            if (!file.createNewFile()) {
                throw new CustomException(Constants.ERROR,
                    "Failed to create temporary file: " + file.getAbsolutePath(),
                    HttpStatus.INTERNAL_SERVER_ERROR);
            }
            // Use try-with-resources to ensure FileOutputStream is closed
            try (FileOutputStream fos = new FileOutputStream(file)) {
                fos.write(mFile.getBytes());
            }

            String uploadFolderPath =
                cbServerProperties.getDiscussionCloudFolderName() + Constants.PATH_DELIMITER + communityId;
            return uploadFile(file, uploadFolderPath,
                cbServerProperties.getDiscussionContainerName());
        } catch (Exception e) {
            log.error(FAILED_UPLOAD_FILE_LOG, e);
            response.getParams().setStatus(Constants.FAILED);
            response.getParams().setErrMsg(FAILED_UPLOAD_FILE_LOG + e.getMessage());
            response.setResponseCode(HttpStatus.INTERNAL_SERVER_ERROR);
            return response;
        } finally {
            if (file != null && file.exists()) {
                try {
                    Files.delete(file.toPath());
                } catch (IOException e) {
                    log.error("Failed to delete temporary file: {}", file.getAbsolutePath(), e);
                }
            }
        }
    }

    @Override
    public ApiResponse searchTopic(SearchCriteria searchCriteria) {
        log.info("CommunityEngagementService:searchTopic::inside method");
        ApiResponse response = ProjectUtil.createDefaultResponse(Constants.API_COMMUNITY_SEARCH);
        try {
            SearchResult searchResult = redisTemplate.opsForValue()
                .get(generateRedisJwtTokenKey(searchCriteria));
            if (searchResult != null) {
                log.info(
                    "DiscussionServiceImpl::searchDiscussion:  search result fetched from redis");
                response.getResult().put(Constants.SEARCH_RESULTS, searchResult);
                createSuccessResponse(response);
                return response;
            }
            String searchString = searchCriteria.getSearchString();
            if (searchString != null && searchString.length() < 2) {
                createErrorResponse(response, Constants.MINIMUM_CHARACTERS_NEEDED,
                    HttpStatus.BAD_REQUEST, Constants.FAILED_CONST);
                return response;
            }
            return handleSearchAndCache(searchCriteria, response, communityCategoryIndex);
        } catch (Exception e) {
            logger.error(ERROR_SEARCHING_LOG, e);
            throw new CustomException(Constants.ERROR, Constants.ERROR_WHILE_PROCESSING,
                HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    @Override
    public ApiResponse listAllCommunitiesJoinedByUser(String authToken) {
        log.info("CommunityEngagementService:listAllCommunitiesJoinedByUser::inside method");
        ApiResponse response = ProjectUtil.createDefaultResponse(
            Constants.API_LIST_ALL_COMMUNITIES_JOINED);
        try {
            String userId = accessTokenValidator.verifyUserToken(authToken);
            if (StringUtils.isBlank(userId)) {
                response.getParams().setErrMsg(Constants.USER_ID_DOESNT_EXIST);
                response.setResponseCode(HttpStatus.BAD_REQUEST);
                return response;
            }

            Map<String, Object> propertyMap = new HashMap<>();
            propertyMap.put(Constants.USER_ID, userId);
            List<String> fields = Arrays.asList(Constants.COMMUNITY_ID, Constants.STATUS);
            List<Map<String, Object>> userCommunityDetails = cassandraOperation.getRecordsByPropertiesWithoutFiltering(
                Constants.KEYSPACE_SUNBIRD, Constants.USER_COMMUNITY_TABLE, propertyMap, fields,
                null);

            List<Object> communityEntityList = new ArrayList<>();
            if (!userCommunityDetails.isEmpty()) {
                for (Map<String, Object> communityDetail : userCommunityDetails) {
                    Boolean status = (Boolean) communityDetail.get(Constants.STATUS);
                    if (Boolean.TRUE.equals(status)) {
                        String communityId = (String) communityDetail.get(
                            Constants.COMMUNITY_ID_LOWERCASE);
                        String cachedJson = cacheService.getCache(communityId);
                        Map<String, Object> communityMap = null;

                        if (StringUtils.isNotEmpty(cachedJson)) {
                            communityMap = parseCommunityMap(cachedJson);
                        } else {
                            Optional<CommunityEntity> communityEntityOptional = communityEngagementRepository.findByCommunityIdAndIsActive(
                                communityId, true);
                            if (communityEntityOptional.isPresent()) {
                                communityMap = objectMapper.convertValue(
                                    communityEntityOptional.get().getData(),
                                    new TypeReference<Map<String, Object>>() {
                                    });
                                cacheService.putCache(communityId,
                                    communityEntityOptional.get().getData());
                            }
                        }

                        if (communityMap != null && communityMap.containsKey(
                            Constants.COMMUNITY_NAME) && communityMap.containsKey(
                            Constants.COMMUNITY_ID)) {
                            Map<String, String> resultMap = new HashMap<>();
                            resultMap.put(Constants.COMMUNITY_NAME,
                                (String) communityMap.get(Constants.COMMUNITY_NAME));
                            resultMap.put(Constants.COMMUNITY_ID_LOWERCASE,
                                (String) communityMap.get(Constants.COMMUNITY_ID));
                            communityEntityList.add(resultMap);
                        }
                    }
                }
            }

            response.getResult().put(Constants.COMMUNITY_ID,
                objectMapper.convertValue(communityEntityList, new TypeReference<Object>() {
                }));
            return response;

        } catch (Exception e) {
            logger.error("Error while fetching all the communities joined by loggedIn user: {}",
                e.getMessage(), e);
            throw new CustomException(Constants.ERROR, Constants.ERROR_WHILE_PROCESSING,
                HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    private Map<String, Object> parseCommunityMap(String cachedJson) throws JsonProcessingException {
        return objectMapper.readValue(cachedJson, new TypeReference<Map<String, Object>>() {});
    }

    @Override
    public ApiResponse publish(JsonNode communityDetails, String authToken) {
        log.info("CommunityEngagementService:publish::inside method");
        ApiResponse response = ProjectUtil.createDefaultResponse(Constants.API_COMMUNITY_PUBLISH);
        String userId = accessTokenValidator.verifyUserToken(authToken);
        if (StringUtils.isBlank(userId)) {
            response.getParams().setStatus(Constants.FAILED);
            response.getParams().setErrMsg(Constants.USER_ID_DOESNT_EXIST);
            response.setResponseCode(HttpStatus.BAD_REQUEST);
            return response;
        }
        try {
            payloadValidation.validatePayload(Constants.COMMUNITY_PUBLISH_PAYLOAD_VALIDATION_FILE, communityDetails);
            if (esUtilService.isDuplicateCommunity(communityDetails.get(Constants.ORG_ID).asText(),
                communityDetails.get(Constants.COMMUNITY_NAME).asText(), communityDetails.get(Constants.COMMUNITY_ID).asText())) {
                response.getParams().setStatus(Constants.FAILED);
                response.getParams().setErrMsg(Constants.CREATE_ERROR_MSG_WITHIN_COMMUNITY);
                response.setResponseCode(HttpStatus.CONFLICT);
                return response;
            }
            boolean isCommunityCreationAllowed = false; // Default value
            if (communityDetails.has(Constants.CommunityCreationAllowed)) {
                isCommunityCreationAllowed = communityDetails.get(
                    Constants.CommunityCreationAllowed).asBoolean();
            }

            if (!isCommunityCreationAllowed && Boolean.TRUE.equals(esUtilService.doesCommunityNameExistForPublish(
                communityDetails.get(Constants.COMMUNITY_NAME).asText(),
                communityDetails.get(Constants.COMMUNITY_ID).asText()))) {
                response.getParams().setStatus(Constants.FAILED);
                response.getParams().setErrMsg(
                    Constants.CREATE_ERROR_MSG_COMMUNITY);
                response.setResponseCode(HttpStatus.PRECONDITION_FAILED);
                return response;
            }
        } catch (CustomException e) {
            log.error(VALIDATION_FAILED_LOG, e.getMessage(), e);
            response.getParams().setStatus(Constants.FAILED);
            response.getParams().setErrMsg(e.getMessage());
            response.setResponseCode(HttpStatus.BAD_REQUEST);
            return response;
        }
        try {
            String communityId = communityDetails.get(Constants.COMMUNITY_ID).asText();
            Optional<CommunityEntity> communityEntityOptional = communityEngagementRepository.findByCommunityIdAndIsActive(
                communityId, true);
            if (!communityEntityOptional.isPresent()) {
                response.getParams().setErrMsg(Constants.INVALID_COMMUNITY_ID);
                response.setResponseCode(HttpStatus.BAD_REQUEST);
                return response;
            }
            JsonNode dataNode = communityEntityOptional.get().getData();
            Iterator<Map.Entry<String, JsonNode>> fields = communityDetails.fields();
            while (fields.hasNext()) {
                Map.Entry<String, JsonNode> field = fields.next();
                String fieldName = field.getKey();
                // Check if the field is present in the update JsonNode
                if (dataNode.has(fieldName)) {
                    // Update the main JsonNode with the value from the update JsonNode
                    ((ObjectNode) dataNode).set(fieldName, communityDetails.get(fieldName));
                } else {
                    ((ObjectNode) dataNode).put(fieldName, communityDetails.get(fieldName));
                }
            }
            Timestamp currentTime = new Timestamp(System.currentTimeMillis());
            ((ObjectNode) dataNode).put(Constants.PUBLISHED_ON, String.valueOf(currentTime));
            ((ObjectNode) dataNode).put(Constants.PUBLISHED_BY, userId);
            updateCommunityDetails(communityEntityOptional.get(), userId, dataNode,
                Constants.ACTIVE);
            response.getResult().put(Constants.RESPONSE,
                "Published the community with id: " + communityId);
            cacheService.deleteCache(generateRedisJwtTokenKey(createDefaultSearchPayload()));
            cacheService.deleteCache(
                generateRedisJwtTokenKey(createDefaultSearchCriteriaForTopic()));
            if (dataNode.hasNonNull(Constants.MODERATORS) && dataNode.get(Constants.MODERATORS).isArray()) {
                JsonNode moderatorsNode = dataNode.get(Constants.MODERATORS);
                List<String> moderatorIds = new ArrayList<>();
                for (JsonNode moderator : moderatorsNode) {
                    if (moderator.hasNonNull(Constants.MODERATOR_ID)) {
                        moderatorIds.add(moderator.get(Constants.MODERATOR_ID).asText());
                    }
                }
                notificationService.sendNotification(moderatorIds, communityId, userId, dataNode.get(Constants.COMMUNITY_NAME).asText());
                // Now you have a list of moderatorIds
            }
            return response;


        } catch (Exception e) {
            logger.error("Error while publishing community {}: {}",
                communityDetails.has(Constants.COMMUNITY_ID), e.getMessage(), e);
            throw new CustomException(Constants.ERROR, Constants.ERROR_WHILE_PROCESSING,
                HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    @Override
    public ApiResponse searchCommunityFromPrimary(SearchCriteria searchCriteria) {
        log.info("CommunityEngagementService:searchCommunity::inside method");
        ApiResponse response = ProjectUtil.createDefaultResponse(Constants.API_COMMUNITY_SEARCH);
        try {
            String searchString = searchCriteria.getSearchString();
            if (searchString != null && searchString.length() < 2) {
                createErrorResponse(response, Constants.MINIMUM_CHARACTERS_NEEDED,
                    HttpStatus.BAD_REQUEST, Constants.FAILED_CONST);
                return response;
            }
            return searchCommunityFromEs(searchCriteria, response);
        } catch (RuntimeException e) {
            log.error("Validation error in search:", e);
            createErrorResponse(response, e.getMessage(),
                    HttpStatus.BAD_REQUEST, Constants.FAILED_CONST);
            return response;
        } catch (Exception e) {
            logger.error(ERROR_SEARCHING_LOG, e);
            throw new CustomException(Constants.ERROR, Constants.ERROR_WHILE_PROCESSING,
                HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    @Override
    public ApiResponse syncUserWithCommunity(MultipartFile file) {
        log.info("CommunityEngagementService:syncUserWithCommunity::inside method");
        ApiResponse response = ProjectUtil.createDefaultResponse(Constants.API_COMMUNITY_SEARCH);
        try {

            List<Map<String, String>> processedData = validateFileAndProcessRows(file);

            List<Map<String, Object>> records = objectMapper.convertValue(
                processedData,
                new TypeReference<List<Map<String, Object>>>() {
                }
            );
            if (records.isEmpty()) {
                log.info("No records found in the table: {}.{}");
            } else {
                // Process the fetched records
                for (Map<String, Object> fileRecord : records) {
                    // Add your processing logic here
                    // Check if the record contains the key 'status' and if its value is true
                    if (fileRecord.containsKey("status") && Constants.TRUE.equalsIgnoreCase(
                        (String) fileRecord.get("status"))) {
                        String userId = (String) fileRecord.get(
                            Constants.USER_ID_LOWER_CASE); // Fetch userId from the record
                        String communityId = (String) fileRecord.get(
                            Constants.COMMUNITY_ID_LOWERCASE); // Fetch communityId from the record
                        esUtilService.updateUserIndex(userId, communityId, true);
                        // Add your processing logic here
                    } else {
                        log.info("Skipping record: {}", fileRecord);
                    }
                }
            }

            response.getParams().setStatus(Constants.SUCCESS);
            response.getParams().setErrMsg("User sync completed successfully.");
            response.setResponseCode(HttpStatus.OK);

        } catch (Exception e) {
            logger.error("Error occurred while syncing users with community:", e);
            throw new CustomException(Constants.ERROR, Constants.ERROR_WHILE_PROCESSING,
                HttpStatus.INTERNAL_SERVER_ERROR);
        }
        return response;
    }

    private List<Map<String, String>> validateFileAndProcessRows(MultipartFile file) {
        log.info("CommunityServiceImpl::validateFileAndProcessRows");
        String fileName = file.getOriginalFilename();
        if (fileName == null) {
            throw new CustomException(Constants.ERROR, "File name is null", HttpStatus.BAD_REQUEST);
        }

        try (InputStream inputStream = file.getInputStream()) {
            if (fileName.endsWith(".csv")) {
                return fileProcessService.processCsvAndSendMessage(inputStream);
            } else {
                throw new CustomException(Constants.ERROR, "Unsupported file type: " + fileName, HttpStatus.BAD_REQUEST);
            }
        } catch (IOException e) {
            log.error("Error while processing file: {}", e.getMessage());
            throw new CustomException(Constants.ERROR, Constants.ERROR_WHILE_PROCESSING, HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    private ApiResponse searchCommunityFromEs(SearchCriteria searchCriteria, ApiResponse response) {
        try {
            SearchResult searchResult = esUtilService.searchDocuments(Constants.INDEX_NAME,
                searchCriteria);
            if (!searchResult.getData().isEmpty()) {
                Set<String> uniqueOrgIds = new HashSet<>();
                // Extract 'data' field from searchResult
                JsonNode dataNode = searchResult.getData();
                if (dataNode != null && dataNode.isArray()) {
                    for (JsonNode item : dataNode) {
                        if (item.has(Constants.CREATED_BY) && !item.get(Constants.CREATED_BY)
                            .isNull()) {
                            JsonNode createdByNode = item.get(Constants.CREATED_BY);
                            if (createdByNode.isTextual()) {
                                uniqueOrgIds.add(Constants.USER_PREFIX
                                    + createdByNode.asText()); // Add prefix directly
                            }
                        }
                    }
                }
                List<String> userIdList = new ArrayList<>(
                    uniqueOrgIds != null ? uniqueOrgIds : Collections.emptySet());
// Convert Set to List
                List<Object> userList = fetchDataForKeys(userIdList);
                if (userList != null) {
                    userList.replaceAll(obj -> {
                        if (obj instanceof Map<?, ?> rawMap) {
                            @SuppressWarnings("unchecked")
                            Map<String, Object> map = (Map<String, Object>) rawMap;
                            map.computeIfPresent(Constants.DESIGNATION, (k, v) ->
                                Constants.NULL_STRING.equalsIgnoreCase(String.valueOf(v)) ? "" : v);
                        }
                        return obj;
                    });
                    List<Map<String, Object>> userInfoList = userList.stream()
                        .filter(Map.class::isInstance) // Ensure the object is a Map
                        .map(obj -> (Map<String, Object>) obj) // Cast to Map<String, Object>
                        .toList();
                    if (userInfoList != null) {
                        searchResult.setAdditionalInfo(userInfoList);
                    }
                }

            }
            response.getResult().put(Constants.SEARCH_RESULTS, searchResult);
            createSuccessResponse(response);
            return response;
        } catch (Exception e) {
            logger.error("Exception occured while fetching and caching in search API:", e);
            throw new CustomException(Constants.ERROR, "error while processing ",
                HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    public ApiResponse uploadFile(File file, String cloudFolderName, String containerName) {
        ApiResponse response = ProjectUtil.createDefaultResponse(Constants.UPLOAD_FILE);
        try {
            String objectKey = cloudFolderName + Constants.PATH_DELIMITER + file.getName();
            String url = storageService.upload(containerName, file.getAbsolutePath(),
                objectKey, Option.apply(false), Option.apply(1), Option.apply(5), Option.empty());
            Map<String, String> uploadedFile = new HashMap<>();
            uploadedFile.put(Constants.NAME, file.getName());
            uploadedFile.put(Constants.URL, url);
            response.getResult().putAll(uploadedFile);
            return response;
        } catch (Exception e) {
            log.error(FAILED_UPLOAD_FILE_LOG, e);
            response.getParams().setStatus(Constants.FAILED);
            response.getParams().setErrMsg(FAILED_UPLOAD_FILE_LOG + e.getMessage());
            response.setResponseCode(HttpStatus.INTERNAL_SERVER_ERROR);
            return response;
        }
    }

    private String validateReportPayload(Map<String, Object> reportData) {
        StringBuilder errorMsg = new StringBuilder();
        List<String> errList = new ArrayList<>();

        if (reportData.containsKey(Constants.COMMUNITY_ID) && StringUtils.isBlank(
            (String) reportData.get(Constants.COMMUNITY_ID))) {
            errList.add(Constants.COMMUNITY_ID);
        }
        if (reportData.containsKey(Constants.REPORTED_REASON)) {
            Object reportedReasonObj = reportData.get(Constants.REPORTED_REASON);
            if (reportedReasonObj instanceof List<?> rawReportedReasonList) {
                @SuppressWarnings("unchecked")
                List<String> reportedReasonList = (List<String>) rawReportedReasonList;
                if (reportedReasonList.isEmpty()) {
                    errList.add(Constants.REPORTED_REASON);
                } else if (reportedReasonList.contains(Constants.OTHERS) && (!reportData.containsKey(Constants.OTHER_REASON) ||
                    StringUtils.isBlank((String) reportData.get(Constants.OTHER_REASON)))) {
                    errList.add(Constants.OTHER_REASON);
                }
            } else {
                errList.add(Constants.REPORTED_REASON);
            }
        }
        if (!errList.isEmpty()) {
            errorMsg.append("Failed Due To Missing Params - ").append(errList).append(".");
        }
        return errorMsg.toString();
    }


    private CommunityCategory persistCategoryInPrimary(JsonNode categoryDetails, Integer parentId,
        String userId, Timestamp currentTimestamp, String userRootOrgId) {
        log.info("CommunityEngagementService:persistCategoryInPimaryAndEs:saving, userId {}", userId);
        CommunityCategory communityCategory = new CommunityCategory();
        communityCategory.setCategoryName(categoryDetails.get(Constants.CATEGORY_NAME).asText());
        communityCategory.setDescription(categoryDetails.get(Constants.DESCRIPTION).asText());
        communityCategory.setParentId(parentId);
        communityCategory.setCreatedAt(currentTimestamp);
        communityCategory.setCountOfCommunities(0L);
        communityCategory.setDepartmentId(userRootOrgId);

        // Save to the repository and fetch the generated ID
        return categoryRepository.save(communityCategory);

    }



    private ApiResponse handleSearchAndCache(SearchCriteria searchCriteria, ApiResponse response, String indexName) {
        try {
            SearchResult searchResult = esUtilService.searchDocuments(indexName,
                searchCriteria);
            if (!searchResult.getData().isEmpty()) {
                Set<String> uniqueOrgIds = new HashSet<>();
                // Extract 'data' field from searchResult
                JsonNode dataNode = searchResult.getData();
                if (dataNode != null && dataNode.isArray()) {
                    for (JsonNode item : dataNode) {
                        if (item.has(Constants.ORD_ID) && !item.get(Constants.ORD_ID).isNull()) {
                            JsonNode orgIdNode = item.get(Constants.ORD_ID);
                            if (orgIdNode.isTextual()) {
                                uniqueOrgIds.add(orgIdNode.asText());
                            }
                        }
                    }
                }
                // Convert Set to List
                List<String> orgIdList = new ArrayList<>(uniqueOrgIds);
                Map<String, Object> propertyMap = new HashMap<>();
                propertyMap.put(Constants.ID, orgIdList);
                enrichOrgInfo(searchResult, uniqueOrgIds, orgIdList);

            }

            redisTemplate.opsForValue().set(
                generateRedisJwtTokenKey(searchCriteria),
                searchResult,
                cbServerProperties.getSearchResultRedisTtl(),
                TimeUnit.SECONDS
            );
            response.getResult().put(Constants.SEARCH_RESULTS, searchResult);
            createSuccessResponse(response);
            return response;
        } catch (Exception e) {
            logger.error("Exception occured while fetching and caching in search API:", e);
            throw new CustomException(Constants.ERROR, Constants.ERROR_WHILE_PROCESSING,
                HttpStatus.INTERNAL_SERVER_ERROR);
        }

    }

    private void enrichOrgInfo(SearchResult searchResult, Set<String> uniqueOrgIds, List<String> orgIdList) {
        List<Object> redisResults = fetchDataForKeys(
            orgIdList.stream().map(id -> Constants.ORG_REDIX_KEY + id).toList()
        );
        List<Map<String, Object>> orgInfoList = redisResults.stream()
            .filter(Map.class::isInstance) // Ensure the object is a Map
            .map(obj -> (Map<String, Object>) obj) // Cast to Map<String, Object>
            .collect(Collectors.toList());
        // Remove found IDs from orgIdSet
        redisResults.forEach(obj -> {
            if (obj instanceof Map<?, ?> map) {
                Object idValue = map.get(Constants.ID);
                if (idValue != null) {
                    uniqueOrgIds.remove(idValue.toString()); // Remove if found
                }
            }
        });

        // If any IDs are still missing, perform some operation
        if (!uniqueOrgIds.isEmpty()) {
            // Example: Fetch missing IDs from Cassandra
            Map<String, Object> missingPropertyMap = new HashMap<>();
            missingPropertyMap.put(Constants.ID, new ArrayList<>(uniqueOrgIds));

            List<Map<String, Object>> missingOrgInfoList = cassandraOperation.getRecordsByPropertiesWithoutFiltering(
                Constants.KEYSPACE_SUNBIRD, Constants.TABLE_ORGANISATION, missingPropertyMap,
                Arrays.asList(Constants.LOGO, Constants.ORG_NAME, Constants.ID), null
            );
            orgInfoList.addAll(missingOrgInfoList);
        }
        searchResult.setAdditionalInfo(orgInfoList);
    }

    private void createSuccessResponse(ApiResponse response) {
        response.setParams(new ApiRespParam());
        response.getParams().setStatus(Constants.SUCCESS);
        response.setResponseCode(HttpStatus.OK);
    }

    public void createErrorResponse(ApiResponse response, String errorMessage,
        HttpStatus httpStatus, String status) {
        response.setParams(new ApiRespParam());
        response.getParams().setErrMsg(errorMessage);
        response.getParams().setStatus(status);
        response.setResponseCode(httpStatus);
    }

    private String generateRedisJwtTokenKey(SearchCriteria requestPayload) {
        if (requestPayload != null) {
            try {
                String reqJsonString = objectMapper.writeValueAsString(requestPayload);
                return JWT.create().withClaim(Constants.REQUEST_PAYLOAD, reqJsonString).sign(
                    Algorithm.HMAC256(Constants.JWT_SECRET_KEY));
            } catch (JsonProcessingException e) {
                log.error("Error occurred while converting json object to json string: {}", e.getMessage(), e);
            }
        }
        return "";
    }

    private ApiResponse returnErrorMsg(String error, HttpStatus httpStatus, ApiResponse response) {
        response.setResponseCode(httpStatus);
        response.getParams().setErr(error);
        return response;
    }

    private String validateJoinPayload(Map<String, Object> request) {
        StringBuilder str = new StringBuilder();
        List<String> errList = new ArrayList<>();

        if (request.containsKey(Constants.COMMUNITY_ID) &&
            StringUtils.isBlank((String) request.get(Constants.COMMUNITY_ID))) {
            errList.add(Constants.COMMUNITY_ID);
        }
        if (!errList.isEmpty()) {
            str.append("Failed Due To Missing Params - ").append(errList).append(".");
        }
        return str.toString();
    }

    @Override
    public ApiResponse read(String communityId) {
        log.info("CommunityEngagementService:read:reading community");
        ApiResponse response = ProjectUtil.createDefaultResponse(Constants.API_ORG_BOOKMARK_READ);
        if (StringUtils.isEmpty(communityId)) {
            logger.error(Constants.COMMUNITY_ID_NOT_FOUND);
            response.setResponseCode(HttpStatus.INTERNAL_SERVER_ERROR);
            response.getParams().setErrMsg(Constants.ID_NOT_FOUND);
            return response;
        }
        try {
            String cachedJson = cacheService.getCache(communityId);
            if (StringUtils.isNotEmpty(cachedJson)) {
                log.info(RECORD_FROM_REDIS_CACHE_LOG);
                response.getParams().setErrMsg(Constants.SUCCESSFULLY_READING);
                response
                        .getResult()
                        .put(Constants.COMMUNITY_DETAILS, objectMapper.readValue(cachedJson, new TypeReference<Object>() {
                        }));
            } else {
                Optional<CommunityEntity> communityEntityOptional = communityEngagementRepository.findByCommunityIdAndIsActive(communityId, true);
                if (communityEntityOptional.isPresent()) {
                    CommunityEntity communityEntity = communityEntityOptional.get();
                    cacheService.putCache(communityEntity.getCommunityId(),
                            communityEntityOptional.get().getData());
                    log.info(RECORD_FROM_POSTGRES_LOG);
                    response.getParams().setErrMsg(Constants.SUCCESSFULLY_READING);
                    response.getResult().put(Constants.COMMUNITY_DETAILS, objectMapper.convertValue(communityEntity.getData(), new TypeReference<Object>() {
                    }));
                } else {
                    logger.error(INVALID_ID_LOG, communityId);
                    response.setResponseCode(HttpStatus.NOT_FOUND);
                    response.getParams().setErrMsg(Constants.INVALID_COMMUNITY_ID);
                }
            }

        } catch (Exception e) {
            logger.error("Error while mapping JSON for id {}: {}", communityId, e.getMessage(), e);
            throw new CustomException(Constants.ERROR, Constants.ERROR_WHILE_PROCESSING, HttpStatus.INTERNAL_SERVER_ERROR);
        }
        return response;
    }

    @Override
    public ApiResponse adminJoinCommunity(Map<String, Object> request, String authToken) {

        logger.info("CommunityManagementServiceImpl:adminJoinCommunity");
        ApiResponse response = ProjectUtil.createDefaultResponse(Constants.API_COMMUNITY_ADMIN_JOIN);
        try {
            CommunityEntity community = validateAdminCommunityRequest(request, authToken, response);
            if (community == null) {
                return response;
            }
            String communityId = (String) request.get(Constants.COMMUNITY_ID);
            Instant now = Instant.now();

            List<String> joinedUsers = new ArrayList<>();
            List<String> alreadyJoinedUsers = new ArrayList<>();
            List<String> failedUsers = new ArrayList<>();

            List<String> userIds = (List<String>) request.get(Constants.USER_IDS);
            for (String userId : userIds) {
                String status = joinUserToCommunity(userId, communityId, community, now);
                switch (status) {
                    case Constants.JOINED_UPPER:
                        joinedUsers.add(userId);
                        break;
                    case Constants.ALREADY_JOINED_UPPER:
                        alreadyJoinedUsers.add(userId);
                        break;
                    default:
                        failedUsers.add(userId);
                }
            }
            if (!joinedUsers.isEmpty()) {
                clearCommunityCache();
            }
            Map<String, Object> result = new HashMap<>();
            result.put(Constants.JOINED_USERS, joinedUsers);
            result.put(Constants.ALREADY_JOINED_USERS, alreadyJoinedUsers);
            result.put(Constants.FAILED_USERS, failedUsers);
            response.setResult(result);
            return response;

        } catch (Exception e) {
            logger.error("Error while admin joining users to community", e);
            throw new CustomException(Constants.ERROR, Constants.ERROR_WHILE_PROCESSING, HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    private String joinUserToCommunity(String userId, String communityId, CommunityEntity community, Instant now) {

        try {
            Map<String, Object> propertyMap = new HashMap<>();
            propertyMap.put(Constants.USER_ID, userId);
            propertyMap.put(Constants.COMMUNITY_ID, communityId);
            List<Map<String, Object>> userCommunityDetails =
                    cassandraOperation.getRecordsByPropertiesWithoutFiltering(
                            Constants.KEYSPACE_SUNBIRD,
                            Constants.USER_COMMUNITY_TABLE,
                            propertyMap,
                            null,
                            1);

            if (CollectionUtils.isEmpty(userCommunityDetails)) {
                Map<String, Object> insertMap = new HashMap<>();
                insertMap.put(Constants.COMMUNITY_ID, communityId);
                insertMap.put(Constants.USER_ID, userId);
                insertMap.put(Constants.STATUS, true);
                insertMap.put(Constants.LAST_UPDATED_AT, now);
                cassandraOperation.insertRecord(Constants.KEYSPACE_SUNBIRD, Constants.USER_COMMUNITY_TABLE, insertMap);

            } else {
                Boolean status = (Boolean) userCommunityDetails.get(0).get(Constants.STATUS);
                if (Boolean.TRUE.equals(status)) {
                    return Constants.ALREADY_JOINED_UPPER;
                }

                Map<String, Object> updateMap = new HashMap<>();
                updateMap.put(Constants.STATUS, true);
                updateMap.put(Constants.LAST_UPDATED_AT, now);
                cassandraOperation.updateRecord(Constants.KEYSPACE_SUNBIRD, Constants.USER_COMMUNITY_TABLE, updateMap, propertyMap);
            }

            Map<String, Object> dataMap = new HashMap<>();
            dataMap.put(Constants.COMMUNITY, community);
            dataMap.put(Constants.USER_ID, userId);
            producer.push(userCountUpdateTopic, dataMap);
            esUtilService.updateUserIndex(userId, communityId, true);
            return Constants.JOINED_UPPER;

        } catch (Exception e) {
            logger.error("Error while adding user {} to community {}", userId, communityId, e);
            return Constants.FAILED_CONST;
        }
    }

    @Override
    public ApiResponse adminUnjoinCommunity(Map<String, Object> request, String authToken) {

        logger.info("CommunityManagementServiceImpl:adminUnjoinCommunity");
        ApiResponse response = ProjectUtil.createDefaultResponse(Constants.API_COMMUNITY_ADMIN_UNJOIN);
        try {
            CommunityEntity community = validateAdminCommunityRequest(request, authToken, response);
            if (community == null) {
                return response;
            }
            String communityId = (String) request.get(Constants.COMMUNITY_ID);
            Instant now = Instant.now();
            List<String> unjoinedUsers = new ArrayList<>();
            List<String> notJoinedUsers = new ArrayList<>();
            List<String> failedUsers = new ArrayList<>();

            List<String> userIds = (List<String>) request.get(Constants.USER_IDS);
            for (String userId : userIds) {
                String status = unjoinUserFromCommunity(userId, communityId, community, now);
                switch (status) {
                    case Constants.UNJOINED_UPPER:
                        unjoinedUsers.add(userId);
                        break;
                    case Constants.NOT_JOINED_UPPER:
                        notJoinedUsers.add(userId);
                        break;
                    default:
                        failedUsers.add(userId);
                }
            }
            if (!unjoinedUsers.isEmpty()) {
                clearCommunityCache();
            }
            Map<String, Object> result = new HashMap<>();
            result.put(Constants.UNJOINED_USERS, unjoinedUsers);
            result.put(Constants.NOT_JOINED_USERS, notJoinedUsers);
            result.put(Constants.FAILED_USERS, failedUsers);
            response.setResult(result);

            return response;

        } catch (Exception e) {
            logger.error("Error while admin removing users from community", e);
            throw new CustomException(Constants.ERROR, Constants.ERROR_WHILE_PROCESSING, HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    private String unjoinUserFromCommunity(String userId, String communityId, CommunityEntity community, Instant now) {
        try {
            Map<String, Object> propertyMap = new HashMap<>();
            propertyMap.put(Constants.USER_ID, userId);
            propertyMap.put(Constants.COMMUNITY_ID, communityId);

            List<Map<String, Object>> userCommunityDetails =
                    cassandraOperation.getRecordsByPropertiesWithoutFiltering(
                            Constants.KEYSPACE_SUNBIRD,
                            Constants.USER_COMMUNITY_TABLE,
                            propertyMap,
                            null,
                            1);

            if (CollectionUtils.isEmpty(userCommunityDetails)) {
                return Constants.NOT_JOINED_UPPER;
            }
            Boolean status = (Boolean) userCommunityDetails.get(0).get(Constants.STATUS);
            if (Boolean.FALSE.equals(status)) {
                return Constants.NOT_JOINED_UPPER;
            }

            Map<String, Object> updateUserCommunityDetails = new HashMap<>();
            updateUserCommunityDetails.put(Constants.STATUS, false);
            updateUserCommunityDetails.put(Constants.LAST_UPDATED_AT, now);

            Map<String, Object> updateUserCommunityLookUp = new HashMap<>();
            updateUserCommunityLookUp.put(Constants.STATUS, false);

            cassandraOperation.updateRecord(Constants.KEYSPACE_SUNBIRD, Constants.USER_COMMUNITY_TABLE, updateUserCommunityDetails, propertyMap);
            cassandraOperation.updateRecord(Constants.KEYSPACE_SUNBIRD, Constants.USER_COMMUNITY_LOOK_UP_TABLE, updateUserCommunityLookUp, propertyMap);

            JsonNode dataNode = community.getData();
            ((ObjectNode) dataNode).put(Constants.COUNT_OF_PEOPLE_JOINED, dataNode.get(Constants.COUNT_OF_PEOPLE_JOINED).asInt() - 1);
            updateCommunityDetails(community, userId, dataNode, Constants.ACTIVE);
            cacheService.deleteUserFromHash(Constants.CMMUNITY_USER_REDIS_PREFIX + communityId, Constants.USER_PREFIX + userId);

            esUtilService.updateUserIndex(userId, communityId, false);
            return Constants.UNJOINED_UPPER;

        } catch (Exception e) {
            logger.error("Error while removing user {} from community {}", userId, communityId, e);
            return Constants.FAILED_CONST;
        }
    }

    private CommunityEntity validateAdminCommunityRequest(
            Map<String, Object> request,
            String authToken,
            ApiResponse response) {

        String adminUserId = accessTokenValidator.verifyUserToken(authToken);
        if (StringUtils.isBlank(adminUserId)) {
            returnErrorMsg("Invalid user ID from auth token.", HttpStatus.BAD_REQUEST, response);
            return null;
        }

        String error = validateJoinPayload(request);
        if (StringUtils.isNotBlank(error)) {
            returnErrorMsg(error, HttpStatus.BAD_REQUEST, response);
            return null;
        }

        error = validateUserIds(request);
        if (StringUtils.isNotBlank(error)) {
            returnErrorMsg(error, HttpStatus.BAD_REQUEST, response);
            return null;
        }

        String communityId = (String) request.get(Constants.COMMUNITY_ID);
        return validateCommunity(communityId, response);
    }

    private String validateUserIds(Map<String, Object> request) {
        Object usersObj = request.get(Constants.USER_IDS);
        if (!(usersObj instanceof List<?> rawUsersList)) {
            return "Invalid userIds";
        }
        @SuppressWarnings("unchecked")
        List<String> usersList = (List<String>) rawUsersList;
        List<String> userIds = new ArrayList<>(new LinkedHashSet<>(usersList));
        if (CollectionUtils.isEmpty(userIds)) {
            return "userIds cannot be empty";
        }
        request.put(Constants.USER_IDS, userIds);
        int maxUsers = cbServerProperties.getCommunityAdminJoinMaxUser();
        if (userIds.size() > maxUsers) {
            return "Maximum " + maxUsers + " users allowed per request";
        }
        return null;
    }

    private CommunityEntity validateCommunity(String communityId, ApiResponse response) {
        Optional<CommunityEntity> optCommunity = communityEngagementRepository.findByCommunityIdAndIsActive(communityId, true);
        if (optCommunity.isEmpty() || optCommunity.get().getData().isEmpty()) {
            response.setResponseCode(HttpStatus.BAD_REQUEST);
            response.getParams().setErr(Constants.INVALID_COMMUNITY_ID);
            return null;
        }
        return optCommunity.get();
    }

    private void clearCommunityCache() {
        cacheService.deleteCache(generateRedisJwtTokenKey(createDefaultSearchPayload()));
    }
}
