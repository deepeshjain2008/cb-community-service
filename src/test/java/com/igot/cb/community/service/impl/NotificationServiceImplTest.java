package com.igot.cb.community.service.impl;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.igot.cb.pores.exceptions.CustomException;
import com.igot.cb.pores.util.CbServerProperties;
import com.igot.cb.pores.util.Constants;
import com.igot.cb.pores.util.OutboundRequestHandlerServiceImpl;
import com.igot.cb.transactional.cassandrautils.CassandraOperation;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class NotificationServiceImplTest {

    @Mock
    private CassandraOperation cassandraOperation;

    @Mock
    private OutboundRequestHandlerServiceImpl outboundRequestHandlerService;

    private CbServerProperties props;
    private NotificationServiceImpl notificationService;

    @BeforeEach
    void setUp() {
        props = new CbServerProperties();
        props.setDomainUrl("https://example.com/");
        props.setFixedCommunityUrl("community/");
        props.setCommunityModeratorTemplate("moderator-template");
        props.setSupportEmail("support@example.com");
        props.setNotifyServiceHost("http://notify-service/");
        props.setNotifyServicePathAsync("send");

        notificationService = new NotificationServiceImpl(cassandraOperation, new ObjectMapper(), props,
            outboundRequestHandlerService);
        ReflectionTestUtils.setField(notificationService, "moderatorMailSubject", "You are a moderator");

        lenient().when(cassandraOperation.getRecordsByPropertiesWithoutFiltering(
                eq(Constants.KEYSPACE_SUNBIRD), eq(Constants.TABLE_EMAIL_TEMPLATE), any(Map.class), anyList(), any()))
            .thenReturn(templateRows("Hello $moderatorName, community $communityName"));
        lenient().when(outboundRequestHandlerService.fetchResultUsingPost(any(String.class), any(), any()))
            .thenReturn(new HashMap<>());
    }

    private List<Map<String, Object>> templateRows(String templateBody) {
        Map<String, Object> row = new HashMap<>();
        row.put(Constants.TEMPLATE, templateBody);
        List<Map<String, Object>> rows = new ArrayList<>();
        rows.add(row);
        return rows;
    }

    private Map<String, Object> userRecord(String id, String firstName, String profileDetailsJson) {
        Map<String, Object> row = new HashMap<>();
        row.put(Constants.ID, id);
        row.put(Constants.FIRST_NAME, firstName);
        row.put(Constants.PROFILE_DETAILS, profileDetailsJson);
        return row;
    }

    private void stubUserRecords(List<Map<String, Object>> records) {
        when(cassandraOperation.getRecordsByPropertiesWithoutFiltering(
                eq(Constants.KEYSPACE_SUNBIRD), eq(Constants.TABLE_USER), any(Map.class), anyList(), any()))
            .thenReturn(records);
    }

    @Test
    void sendNotificationHappyPathSendsEmailToModeratorWithAddress() {
        String senderProfile = "{\"personalDetails\":{\"primaryEmail\":\"sender@example.com\"}}";
        String moderatorProfile = "{\"personalDetails\":{\"primaryEmail\":\"mod@example.com\"}}";
        stubUserRecords(Arrays.asList(
            userRecord("sender1", "Sender", senderProfile),
            userRecord("mod1", "Moderator", moderatorProfile)));

        assertDoesNotThrow(() -> notificationService.sendNotification(
            new ArrayList<>(List.of("mod1")), "community1", "sender1", "My Community"));
    }

    @ParameterizedTest(name = "sendNotificationSkipsRecipientWhenModeratorProfileIs_{0}")
    @MethodSource("skippedModeratorProfiles")
    void sendNotificationSkipsRecipientForInvalidModeratorProfile(String scenario, String moderatorProfile) {
        String senderProfile = "{\"personalDetails\":{\"primaryEmail\":\"sender@example.com\"}}";
        stubUserRecords(Arrays.asList(
            userRecord("sender1", "Sender", senderProfile),
            userRecord("mod1", "Moderator", moderatorProfile)));

        assertDoesNotThrow(() -> notificationService.sendNotification(
            new ArrayList<>(List.of("mod1")), "community1", "sender1", "My Community"));
    }

    private static Stream<Arguments> skippedModeratorProfiles() {
        return Stream.of(
            Arguments.of("blank", "   "),
            Arguments.of("missingPersonalDetails", "{\"otherField\":\"value\"}"),
            Arguments.of("personalDetailsNotAMap", "{\"personalDetails\":\"not-a-map\"}")
        );
    }

    @Test
    void sendNotificationThrowsCustomExceptionForInvalidProfileJson() {
        stubUserRecords(List.of(userRecord("sender1", "Sender", "not-valid-json")));

        List<String> moderatorIds = new ArrayList<>();
        assertThrows(CustomException.class, () -> notificationService.sendNotification(
            moderatorIds, "community1", "sender1", "My Community"));
    }

    @Test
    void sendNotificationThrowsWhenSenderRecordMissingFromResults() {
        String moderatorProfile = "{\"personalDetails\":{\"primaryEmail\":\"mod@example.com\"}}";
        stubUserRecords(List.of(userRecord("mod1", "Moderator", moderatorProfile)));

        List<String> moderatorIds = new ArrayList<>(List.of("mod1"));
        assertThrows(NullPointerException.class, () -> notificationService.sendNotification(
            moderatorIds, "community1", "sender1", "My Community"));
    }

    @Test
    void sendNotificationHandlesMissingEmailTemplateGracefully() {
        when(cassandraOperation.getRecordsByPropertiesWithoutFiltering(
                eq(Constants.KEYSPACE_SUNBIRD), eq(Constants.TABLE_EMAIL_TEMPLATE), any(Map.class), anyList(), any()))
            .thenReturn(Collections.emptyList());
        String senderProfile = "{\"personalDetails\":{\"primaryEmail\":\"sender@example.com\"}}";
        String moderatorProfile = "{\"personalDetails\":{\"primaryEmail\":\"mod@example.com\"}}";
        stubUserRecords(Arrays.asList(
            userRecord("sender1", "Sender", senderProfile),
            userRecord("mod1", "Moderator", moderatorProfile)));

        assertDoesNotThrow(() -> notificationService.sendNotification(
            new ArrayList<>(List.of("mod1")), "community1", "sender1", "My Community"));
    }

    @Test
    void sendNotificationHandlesEmailTemplateLookupException() {
        when(cassandraOperation.getRecordsByPropertiesWithoutFiltering(
                eq(Constants.KEYSPACE_SUNBIRD), eq(Constants.TABLE_EMAIL_TEMPLATE), any(Map.class), anyList(), any()))
            .thenThrow(new RuntimeException("cassandra down"));
        String senderProfile = "{\"personalDetails\":{\"primaryEmail\":\"sender@example.com\"}}";
        String moderatorProfile = "{\"personalDetails\":{\"primaryEmail\":\"mod@example.com\"}}";
        stubUserRecords(Arrays.asList(
            userRecord("sender1", "Sender", senderProfile),
            userRecord("mod1", "Moderator", moderatorProfile)));

        assertDoesNotThrow(() -> notificationService.sendNotification(
            new ArrayList<>(List.of("mod1")), "community1", "sender1", "My Community"));
    }

    @Test
    void sendNotificationHandlesOutboundRequestException() {
        when(outboundRequestHandlerService.fetchResultUsingPost(any(String.class), any(), any()))
            .thenThrow(new RuntimeException("network error"));
        String senderProfile = "{\"personalDetails\":{\"primaryEmail\":\"sender@example.com\"}}";
        String moderatorProfile = "{\"personalDetails\":{\"primaryEmail\":\"mod@example.com\"}}";
        stubUserRecords(Arrays.asList(
            userRecord("sender1", "Sender", senderProfile),
            userRecord("mod1", "Moderator", moderatorProfile)));

        assertDoesNotThrow(() -> notificationService.sendNotification(
            new ArrayList<>(List.of("mod1")), "community1", "sender1", "My Community"));
    }
}
