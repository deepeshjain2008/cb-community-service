package com.igot.cb.community.service.impl;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.igot.cb.community.service.NotificationService;
import com.igot.cb.pores.exceptions.CustomException;
import com.igot.cb.pores.util.*;
import com.igot.cb.transactional.cassandrautils.CassandraOperation;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.velocity.VelocityContext;
import org.apache.velocity.app.VelocityEngine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;

import java.io.StringWriter;
import java.util.*;
import java.util.stream.Collectors;

@Service
@Slf4j
public class NotificationServiceImpl implements NotificationService {

  @Autowired
  CassandraOperation cassandraOperation;

  @Autowired
  ObjectMapper objectMapper;

  @Autowired
  CbServerProperties props;

  @Autowired
  OutboundRequestHandlerServiceImpl outboundRequestHandlerService;

  @Value("${moderator.mail.subject}")
  private String moderatorMailSubject;

  private Logger logger = LoggerFactory.getLogger(NotificationServiceImpl.class);

  @Override
  public void sendNotification(List<String> moderatorIds, String communityId, String userId,
      String communityName) {
    logger.info("NotificationService::sendNotification:Sending notification to moderators");
    List<String> fields = Arrays.asList(Constants.FIRST_NAME);
    moderatorIds.add(userId);
    Map<String, Object> propertiesMap = new HashMap<>();
    propertiesMap.put(Constants.ID, moderatorIds);
    List<Map<String, Object>> userList = cassandraOperation.getRecordsByPropertiesWithoutFiltering(
        Constants.KEYSPACE_SUNBIRD, Constants.TABLE_USER, propertiesMap,
        Arrays.asList(Constants.PROFILE_DETAILS, Constants.FIRST_NAME, Constants.ID), null);
    // Create senderUserMap
    Map<String, Map<String, Object>> senderUserMap = new HashMap<>();
    List<String> emailResponseList = new ArrayList<>();
    Map<String, Map<String, Object>> userListMap = userList.stream()
        .collect(Collectors.toMap(
            userInfo -> (String) userInfo.get(Constants.ID), // Key: userId
            userInfo -> {
              Map<String, Object> userMap = new HashMap<>();
              userMap.put(Constants.FIRST_NAME, userInfo.get(Constants.FIRST_NAME));
              // Process profile details if available
              String profileDetails = (String) userInfo.get(Constants.PROFILE_DETAILS);
              if (StringUtils.isNotBlank(profileDetails)) {
                try {
                  Map<String, Object> profileDetailsMap = objectMapper.readValue(
                      profileDetails, new TypeReference<HashMap<String, Object>>() {
                      });
                  // Extract email if present
                  Optional.ofNullable(profileDetailsMap.get(Constants.PERSONAL_DETAILS))
                      .filter(Map.class::isInstance)
                      .map(Map.class::cast)
                      .map(personalDetails -> personalDetails.get(Constants.PRIMARY_EMAIL))
                      .ifPresent(email -> {
                        userMap.put(Constants.PRIMARY_EMAIL, email);
                        emailResponseList.add((String) email);
                      });
                } catch (Exception e) {
                  logger.error("Error processing user profile details:", e);
                  throw new CustomException(Constants.ERROR, "Error while processing",
                      HttpStatus.INTERNAL_SERVER_ERROR);
                }
              }
              return userMap;
            }
        ));

    if (userListMap.containsKey(userId)) {
      // Fetch and remove user in a single step
      senderUserMap.put(userId, userListMap.remove(userId));
    }
    String link = props.getDomainUrl()+props.getFixedCommunityUrl()+communityId;
    Map<String, Object> mailNotificationDetails = new HashMap<>();
    mailNotificationDetails.put(Constants.SUBJECT, moderatorMailSubject);
    mailNotificationDetails.put(Constants.LINK, link.toString());
    mailNotificationDetails.put(Constants.USER_ID, userId);
    mailNotificationDetails.put(Constants.MDO_LEADER_NAME, senderUserMap.get(userId).get(Constants.FIRST_NAME));
    mailNotificationDetails.put(Constants.COMMUNITY_NAME_TAG, communityName);

    // Iterate through the userListMap and send notifications individually
    userListMap.forEach((id, userMap) -> {
      String email = (String) userMap.get(Constants.PRIMARY_EMAIL);
      if (StringUtils.isNotBlank(email)) {
        mailNotificationDetails.put(Constants.RECIPIENT_EMAILS, Collections.singletonList(email));
        mailNotificationDetails.put(Constants.MODERATOR_NAME, userMap.get(Constants.FIRST_NAME));
        sendNotificationToRecipients(mailNotificationDetails);
      }
    });
  }

  private void sendNotificationToRecipients(Map<String, Object> mailNotificationDetails) {
    logger.info("NotificationService::sendNotificationToRecipients:Sending notification to moderators");
    Map<String, Object> params = new HashMap<>();
    NotificationAsyncRequest notificationRequest = new NotificationAsyncRequest();
    Map<String, Object> action = new HashMap<>();
    Map<String, Object> templ = new HashMap<>();
    Map<String, Object> usermap = new HashMap<>();
    params.put(Constants.LINK, mailNotificationDetails.get(Constants.LINK));
    params.put(Constants.MDO_LEADER_NAME, mailNotificationDetails.get(Constants.MDO_LEADER_NAME));
    params.put(Constants.MODERATOR_NAME, mailNotificationDetails.get(Constants.MODERATOR_NAME));
    params.put(Constants.COMMUNITY_NAME_TAG, mailNotificationDetails.get(Constants.COMMUNITY_NAME_TAG));
    Template template = new Template(constructEmailTemplate(props.getCommunityModeratorTemplate(), params), props.getCommunityModeratorTemplate(), params);
    usermap.put(Constants.ID, mailNotificationDetails.get(Constants.USER_ID));
    usermap.put(Constants.TYPE, Constants.USER);
    action.put(Constants.TEMPLATE, templ);
    action.put(Constants.TYPE, Constants.EMAIL);
    action.put(Constants.CATEGORY, Constants.EMAIL);
    action.put(Constants.CREATED_BY, usermap);
    Config config = new Config();
    config.setSubject((String) mailNotificationDetails.get(Constants.SUBJECT));
    config.setSender(props.getSupportEmail());
    templ.put(Constants.TYPE, Constants.EMAIL);
    templ.put(Constants.DATA, template.getData());
    templ.put(Constants.ID, template.getId());
    templ.put(Constants.PARAMS, params);
    templ.put(Constants.CONFIG, config);
    notificationRequest.setType(Constants.EMAIL);
    notificationRequest.setPriority(1);
    notificationRequest.setIds((List<String>) mailNotificationDetails.get(Constants.RECIPIENT_EMAILS));
    notificationRequest.setAction(action);

    Map<String, Object> req = new HashMap<>();
    Map<String, List<NotificationAsyncRequest>> notificationMap = new HashMap<>();
    notificationMap.put(Constants.NOTIFICATIONS, Collections.singletonList(notificationRequest));
    req.put(Constants.REQUEST, notificationMap);
    sendNotification(req);
  }

  private void sendNotification(Map<String, Object> request) {
    logger.info("NotificationService::sendNotification:Sending notification to moderators");
    StringBuilder builder = new StringBuilder();
    builder.append(props.getNotifyServiceHost()).append(props.getNotifyServicePathAsync());
    try {
      Map<String, Object> response = outboundRequestHandlerService.fetchResultUsingPost(builder.toString(), request, null);
      logger.debug("The email notification is successfully sent, response is: " + response);
    } catch (Exception e) {
      logger.error("Exception while posting the data in notification service: ", e);
    }
  }

  private String constructEmailTemplate(String templateName, Map<String, Object> params) {
    String replacedHTML = new String();
    try {
      Map<String, Object> propertyMap = new HashMap<>();
      propertyMap.put(Constants.NAME, templateName);
      List<Map<String, Object>> templateMap = cassandraOperation.getRecordsByPropertiesWithoutFiltering(Constants.KEYSPACE_SUNBIRD, Constants.TABLE_EMAIL_TEMPLATE, propertyMap, Collections.singletonList(Constants.TEMPLATE), null);
      String htmlTemplate = templateMap.stream()
          .findFirst()
          .map(template -> (String) template.get(Constants.TEMPLATE))
          .orElse(null);
      VelocityEngine velocityEngine = new VelocityEngine();
      velocityEngine.init();
      VelocityContext context = new VelocityContext();
      for (Map.Entry<String, Object> entry : params.entrySet()) {
        context.put(entry.getKey(), entry.getValue());
      }
      StringWriter writer = new StringWriter();
      velocityEngine.evaluate(context, writer, "HTMLTemplate", htmlTemplate);
      replacedHTML = writer.toString();
    } catch (Exception e) {
      logger.error("Unable to create template ", e);
    }
    return replacedHTML;
  }
}
