package com.igot.cb.community.service.impl;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.igot.cb.community.service.UserService;
import com.igot.cb.pores.exceptions.CustomException;
import com.igot.cb.pores.util.Constants;
import com.igot.cb.transactional.cassandrautils.CassandraOperation;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Service
@Slf4j
public class UserServiceImpl implements UserService {
  private final CassandraOperation cassandraOperation;

  private Logger logger = LoggerFactory.getLogger(UserServiceImpl.class);

  private final ObjectMapper objectMapper;

  @Autowired
  public UserServiceImpl(CassandraOperation cassandraOperation, ObjectMapper objectMapper) {
    this.cassandraOperation = cassandraOperation;
    this.objectMapper = objectMapper;
  }

  @Override
  public List<Object> fetchUserFromprimary(List<String> userIds) {
    logger.info("UserService::fetchUserFromprimary:inside");
    Map<String, Object> propertyMap = new HashMap<>();
    propertyMap.put(Constants.ID, userIds);
    List<Map<String, Object>> userInfoList = cassandraOperation.getRecordsByPropertiesWithoutFiltering(
        Constants.KEYSPACE_SUNBIRD, Constants.TABLE_USER, propertyMap,
        Arrays.asList(Constants.PROFILE_DETAILS, Constants.FIRST_NAME, Constants.ID, Constants.CHANNEL), null);

    return userInfoList.stream()
        .map(this::buildUserMap)
        .collect(Collectors.toList());
  }

  private Map<String, Object> buildUserMap(Map<String, Object> userInfo) {
    Map<String, Object> userMap = new HashMap<>();

    // Extract user ID and user name
    userMap.put(Constants.USER_ID_KEY, userInfo.get(Constants.ID));
    userMap.put(Constants.FIRST_NAME_KEY, userInfo.get(Constants.FIRST_NAME));
    userMap.put(Constants.DEPARTMENT, userInfo.get(Constants.CHANNEL));

    // Process profile details if present
    String profileDetails = (String) userInfo.get(Constants.PROFILE_DETAILS);
    if (StringUtils.isNotBlank(profileDetails)) {
      enrichWithProfileDetails(userMap, profileDetails);
    }
    return userMap;
  }

  private void enrichWithProfileDetails(Map<String, Object> userMap, String profileDetails) {
    try {
      // Convert JSON profile details to a Map
      Map<String, Object> profileDetailsMap = objectMapper.readValue(
          profileDetails,
          new TypeReference<HashMap<String, Object>>() {
          });
      userMap.put(Constants.PROFILE_IMG_KEY, "");
      userMap.put(Constants.DESIGNATION_KEY, "");
      userMap.put(Constants.PROFILE_STATUS, "");

      if (MapUtils.isNotEmpty(profileDetailsMap)) {
        applyProfileImage(userMap, profileDetailsMap);
        applyDesignation(userMap, profileDetailsMap);
        applyProfileStatus(userMap, profileDetailsMap);
      }
    } catch (Exception e) {
      logger.error("Exception occured while fetching newlyRegistered user's data from cassandra for communityMemberListing:", e);
      throw new CustomException(Constants.ERROR, "error while processing",
          HttpStatus.INTERNAL_SERVER_ERROR);
    }
  }

  private void applyProfileImage(Map<String, Object> userMap, Map<String, Object> profileDetailsMap) {
    if (profileDetailsMap.containsKey(Constants.PROFILE_IMG)
        && StringUtils.isNotBlank((String) profileDetailsMap.get(Constants.PROFILE_IMG))) {
      userMap.put(Constants.PROFILE_IMG_KEY, profileDetailsMap.get(Constants.PROFILE_IMG));
    }
  }

  private void applyDesignation(Map<String, Object> userMap, Map<String, Object> profileDetailsMap) {
    if (!profileDetailsMap.containsKey(Constants.PROFESSIONAL_DETAILS)
        || ObjectUtils.isEmpty(profileDetailsMap.get(Constants.PROFESSIONAL_DETAILS))) {
      return;
    }
    Object professionalDetailsObj = profileDetailsMap.get(Constants.PROFESSIONAL_DETAILS);
    if (professionalDetailsObj instanceof List<?> professionalDetailsList
        && !professionalDetailsList.isEmpty()
        && professionalDetailsList.get(0) instanceof Map<?, ?> firstEntry
        && firstEntry.get(Constants.DESIGNATION) instanceof String designation
        && StringUtils.isNotBlank(designation)) {
      userMap.put(Constants.DESIGNATION_KEY, designation);
    }
  }

  private void applyProfileStatus(Map<String, Object> userMap, Map<String, Object> profileDetailsMap) {
    if (profileDetailsMap.containsKey(Constants.PROFILE_STATUS_KEY)
        && StringUtils.isNotEmpty((String) profileDetailsMap.get(Constants.PROFILE_STATUS_KEY))) {
      userMap.put(Constants.PROFILE_STATUS, profileDetailsMap.get(Constants.PROFILE_STATUS_KEY));
    }
  }
}
