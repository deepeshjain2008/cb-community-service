package com.igot.cb.community.service.impl;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.igot.cb.pores.exceptions.CustomException;
import com.igot.cb.pores.util.Constants;
import com.igot.cb.transactional.cassandrautils.CassandraOperation;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class UserServiceImplTest {

    @Mock
    private CassandraOperation cassandraOperation;

    private UserServiceImpl userService;

    @BeforeEach
    void setUp() {
        userService = new UserServiceImpl(cassandraOperation, new ObjectMapper());
    }

    private Map<String, Object> userInfo(String id, String firstName, String channel, String profileDetails) {
        Map<String, Object> map = new HashMap<>();
        map.put(Constants.ID, id);
        map.put(Constants.FIRST_NAME, firstName);
        map.put(Constants.CHANNEL, channel);
        map.put(Constants.PROFILE_DETAILS, profileDetails);
        return map;
    }

    private void stubCassandraRecords(List<Map<String, Object>> records) {
        when(cassandraOperation.getRecordsByPropertiesWithoutFiltering(
            any(String.class), any(String.class), any(Map.class), anyList(), any()))
            .thenReturn(records);
    }

    @Test
    void fetchUserFromprimaryReturnsEmptyListWhenNoRecords() {
        stubCassandraRecords(new ArrayList<>());

        List<Object> result = userService.fetchUserFromprimary(Arrays.asList("u1"));

        assertTrue(result.isEmpty());
    }

    @Test
    void fetchUserFromprimarySkipsEnrichmentForBlankProfileDetails() {
        stubCassandraRecords(List.of(userInfo("u1", "Alice", "org1", "   ")));

        List<Object> result = userService.fetchUserFromprimary(Arrays.asList("u1"));

        Map<String, Object> userMap = (Map<String, Object>) result.get(0);
        assertEquals("u1", userMap.get(Constants.USER_ID_KEY));
        assertFalse(userMap.containsKey(Constants.PROFILE_IMG_KEY));
    }

    @Test
    void fetchUserFromprimarySkipsApplyStepsForEmptyProfileDetailsMap() {
        stubCassandraRecords(List.of(userInfo("u1", "Alice", "org1", "{}")));

        List<Object> result = userService.fetchUserFromprimary(Arrays.asList("u1"));

        Map<String, Object> userMap = (Map<String, Object>) result.get(0);
        assertEquals("", userMap.get(Constants.PROFILE_IMG_KEY));
        assertEquals("", userMap.get(Constants.DESIGNATION_KEY));
        assertEquals("", userMap.get(Constants.PROFILE_STATUS));
    }

    @Test
    void fetchUserFromprimaryAppliesAllFieldsWhenFullyPopulated() {
        String profileDetails = "{"
            + "\"profileImageUrl\":\"http://img\","
            + "\"professionalDetails\":[{\"designation\":\"Engineer\"}],"
            + "\"profileStatus\":\"active\""
            + "}";
        stubCassandraRecords(List.of(userInfo("u1", "Alice", "org1", profileDetails)));

        List<Object> result = userService.fetchUserFromprimary(Arrays.asList("u1"));

        Map<String, Object> userMap = (Map<String, Object>) result.get(0);
        assertEquals("http://img", userMap.get(Constants.PROFILE_IMG_KEY));
        assertEquals("Engineer", userMap.get(Constants.DESIGNATION_KEY));
        assertEquals("active", userMap.get(Constants.PROFILE_STATUS));
    }

    @Test
    void fetchUserFromprimarySkipsProfileImageWhenBlank() {
        String profileDetails = "{\"profileImageUrl\":\"   \"}";
        stubCassandraRecords(List.of(userInfo("u1", "Alice", "org1", profileDetails)));

        List<Object> result = userService.fetchUserFromprimary(Arrays.asList("u1"));

        Map<String, Object> userMap = (Map<String, Object>) result.get(0);
        assertEquals("", userMap.get(Constants.PROFILE_IMG_KEY));
    }

    @ParameterizedTest(name = "fetchUserFromprimarySkipsDesignationWhen_{0}")
    @MethodSource("designationSkippedProfileDetails")
    void fetchUserFromprimarySkipsDesignation(String scenario, String profileDetails) {
        stubCassandraRecords(List.of(userInfo("u1", "Alice", "org1", profileDetails)));

        List<Object> result = userService.fetchUserFromprimary(Arrays.asList("u1"));

        Map<String, Object> userMap = (Map<String, Object>) result.get(0);
        assertEquals("", userMap.get(Constants.DESIGNATION_KEY));
    }

    private static Stream<Arguments> designationSkippedProfileDetails() {
        return Stream.of(
            Arguments.of("professionalDetailsMissing", "{\"profileImageUrl\":\"http://img\"}"),
            Arguments.of("professionalDetailsEmpty", "{\"professionalDetails\":[]}"),
            Arguments.of("professionalDetailsNotAList", "{\"professionalDetails\":\"not-a-list\"}"),
            Arguments.of("firstEntryNotAMap", "{\"professionalDetails\":[\"not-a-map\"]}"),
            Arguments.of("designationMissing", "{\"professionalDetails\":[{\"other\":\"value\"}]}"),
            Arguments.of("designationBlank", "{\"professionalDetails\":[{\"designation\":\"   \"}]}")
        );
    }

    @Test
    void fetchUserFromprimarySkipsProfileStatusWhenEmpty() {
        String profileDetails = "{\"profileStatus\":\"\"}";
        stubCassandraRecords(List.of(userInfo("u1", "Alice", "org1", profileDetails)));

        List<Object> result = userService.fetchUserFromprimary(Arrays.asList("u1"));

        Map<String, Object> userMap = (Map<String, Object>) result.get(0);
        assertEquals("", userMap.get(Constants.PROFILE_STATUS));
    }

    @Test
    void fetchUserFromprimaryThrowsCustomExceptionForInvalidJson() {
        stubCassandraRecords(List.of(userInfo("u1", "Alice", "org1", "not-valid-json")));

        List<String> userIds = Arrays.asList("u1");
        assertThrows(CustomException.class, () -> userService.fetchUserFromprimary(userIds));
    }
}
