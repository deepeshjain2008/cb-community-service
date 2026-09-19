package com.igot.cb.community.controller;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.igot.cb.community.service.CommunityManagementService;
import com.igot.cb.pores.elasticsearch.dto.SearchCriteria;
import com.igot.cb.pores.util.ApiResponse;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.mock.web.MockMultipartFile;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class CommunityControllerTest {

    @Mock
    private CommunityManagementService communityManagementService;

    private CommunityController controller;

    private final ObjectMapper mapper = new ObjectMapper();

    @BeforeEach
    void setUp() {
        controller = new CommunityController(communityManagementService);
    }

    private ApiResponse response(HttpStatus status) {
        ApiResponse apiResponse = new ApiResponse();
        apiResponse.setResponseCode(status);
        return apiResponse;
    }

    @Test
    void createDelegatesAndReturnsServiceStatus() {
        JsonNode details = mapper.createObjectNode();
        ApiResponse expected = response(HttpStatus.BAD_REQUEST);
        when(communityManagementService.create(details, "token")).thenReturn(expected);

        ResponseEntity<ApiResponse> result = controller.create(details, "token");

        assertSame(expected, result.getBody());
        assertEquals(HttpStatus.BAD_REQUEST, result.getStatusCode());
    }

    @Test
    void readDelegatesAndAlwaysReturnsOk() {
        ApiResponse expected = response(HttpStatus.INTERNAL_SERVER_ERROR);
        when(communityManagementService.read("c1", "token")).thenReturn(expected);

        ResponseEntity<ApiResponse> result = controller.read("c1", "token");

        assertSame(expected, result.getBody());
        assertEquals(HttpStatus.OK, result.getStatusCode());
    }

    @Test
    void deleteDelegatesAndAlwaysReturnsOk() {
        ApiResponse expected = response(HttpStatus.OK);
        when(communityManagementService.delete("c1", "token")).thenReturn(expected);

        ResponseEntity<ApiResponse> result = controller.delete("c1", "token");

        assertSame(expected, result.getBody());
        assertEquals(HttpStatus.OK, result.getStatusCode());
    }

    @Test
    void updateDelegatesAndReturnsServiceStatus() {
        JsonNode details = mapper.createObjectNode();
        ApiResponse expected = response(HttpStatus.PRECONDITION_FAILED);
        when(communityManagementService.update(details, "token")).thenReturn(expected);

        ResponseEntity<ApiResponse> result = controller.update(details, "token");

        assertSame(expected, result.getBody());
        assertEquals(HttpStatus.PRECONDITION_FAILED, result.getStatusCode());
    }

    @Test
    void joinDelegatesAndReturnsServiceStatus() {
        Map<String, Object> request = new HashMap<>();
        ApiResponse expected = response(HttpStatus.CONFLICT);
        when(communityManagementService.joinCommunity(request, "token")).thenReturn(expected);

        ResponseEntity<ApiResponse> result = controller.join(request, "token");

        assertSame(expected, result.getBody());
        assertEquals(HttpStatus.CONFLICT, result.getStatusCode());
    }

    @Test
    void readJoinDelegatesAndAlwaysReturnsOk() {
        ApiResponse expected = response(HttpStatus.BAD_REQUEST);
        when(communityManagementService.communitiesJoinedByUser("token")).thenReturn(expected);

        ResponseEntity<ApiResponse> result = controller.readJoin("token");

        assertSame(expected, result.getBody());
        assertEquals(HttpStatus.OK, result.getStatusCode());
    }

    @Test
    void listOfUsersJoinedDelegatesAndAlwaysReturnsOk() {
        Map<String, Object> payload = new HashMap<>();
        ApiResponse expected = response(HttpStatus.BAD_REQUEST);
        when(communityManagementService.listOfUsersJoined("token", payload)).thenReturn(expected);

        ResponseEntity<ApiResponse> result = controller.listOfUsersJoined("token", payload);

        assertSame(expected, result.getBody());
        assertEquals(HttpStatus.OK, result.getStatusCode());
    }

    @Test
    void unJoinDelegatesAndReturnsServiceStatus() {
        Map<String, Object> request = new HashMap<>();
        ApiResponse expected = response(HttpStatus.OK);
        when(communityManagementService.unJoinCommunity(request, "token")).thenReturn(expected);

        ResponseEntity<ApiResponse> result = controller.unJoin(request, "token");

        assertSame(expected, result.getBody());
        assertEquals(HttpStatus.OK, result.getStatusCode());
    }

    @Test
    void searchDelegatesAndReturnsServiceStatus() {
        SearchCriteria criteria = new SearchCriteria();
        ApiResponse expected = response(HttpStatus.BAD_REQUEST);
        when(communityManagementService.searchCommunity(criteria)).thenReturn(expected);

        ResponseEntity<ApiResponse> result = controller.search(criteria);

        assertSame(expected, result.getBody());
        assertEquals(HttpStatus.BAD_REQUEST, result.getStatusCode());
    }

    @Test
    void communityCtreateDelegatesAndReturnsServiceStatus() {
        JsonNode details = mapper.createObjectNode();
        ApiResponse expected = response(HttpStatus.OK);
        when(communityManagementService.categoryCreate(details, "token")).thenReturn(expected);

        ResponseEntity<ApiResponse> result = controller.communityCtreate(details, "token");

        assertSame(expected, result.getBody());
        assertEquals(HttpStatus.OK, result.getStatusCode());
    }

    @Test
    void readCategoryByIdDelegatesAndAlwaysReturnsOk() {
        ApiResponse expected = response(HttpStatus.NOT_FOUND);
        when(communityManagementService.readCategory("cat1", "token")).thenReturn(expected);

        ResponseEntity<ApiResponse> result = controller.readCategory("cat1", "token");

        assertSame(expected, result.getBody());
        assertEquals(HttpStatus.OK, result.getStatusCode());
    }

    @Test
    void deleteCategoryDelegatesAndAlwaysReturnsOk() {
        ApiResponse expected = response(HttpStatus.BAD_REQUEST);
        when(communityManagementService.deleteCategory("cat1", "token")).thenReturn(expected);

        ResponseEntity<ApiResponse> result = controller.deleteCategory("cat1", "token");

        assertSame(expected, result.getBody());
        assertEquals(HttpStatus.OK, result.getStatusCode());
    }

    @Test
    void updateCategoryDelegatesAndReturnsServiceStatus() {
        JsonNode details = mapper.createObjectNode();
        ApiResponse expected = response(HttpStatus.CONFLICT);
        when(communityManagementService.updateCategory(details, "token")).thenReturn(expected);

        ResponseEntity<ApiResponse> result = controller.updateCategory(details, "token");

        assertSame(expected, result.getBody());
        assertEquals(HttpStatus.CONFLICT, result.getStatusCode());
    }

    @Test
    void readCategoryListDelegatesAndAlwaysReturnsOk() {
        ApiResponse expected = response(HttpStatus.BAD_REQUEST);
        when(communityManagementService.listOfCategory()).thenReturn(expected);

        ResponseEntity<ApiResponse> result = controller.readCategory();

        assertSame(expected, result.getBody());
        assertEquals(HttpStatus.OK, result.getStatusCode());
    }

    @Test
    void readSubCategoryDelegatesAndAlwaysReturnsOk() {
        SearchCriteria criteria = new SearchCriteria();
        ApiResponse expected = response(HttpStatus.BAD_REQUEST);
        when(communityManagementService.listOfSubCategory(criteria)).thenReturn(expected);

        ResponseEntity<ApiResponse> result = controller.readSubCategory(criteria);

        assertSame(expected, result.getBody());
        assertEquals(HttpStatus.OK, result.getStatusCode());
    }

    @Test
    void readAllCategoryDelegatesAndAlwaysReturnsOk() {
        ApiResponse expected = response(HttpStatus.BAD_REQUEST);
        when(communityManagementService.lisAllCategoryWithSubCat()).thenReturn(expected);

        ResponseEntity<ApiResponse> result = controller.readAllCategory();

        assertSame(expected, result.getBody());
        assertEquals(HttpStatus.OK, result.getStatusCode());
    }

    @Test
    void getTopCommunitiesByFieldDelegatesAndReturnsServiceStatus() {
        Map<String, Object> payload = new HashMap<>();
        ApiResponse expected = response(HttpStatus.OK);
        when(communityManagementService.getPopularCommunitiesByField(payload)).thenReturn(expected);

        ResponseEntity<ApiResponse> result = controller.getTopCommunitiesByField(payload);

        assertSame(expected, result.getBody());
        assertEquals(HttpStatus.OK, result.getStatusCode());
    }

    @Test
    void reportDelegatesAndReturnsServiceStatus() {
        Map<String, Object> reportData = new HashMap<>();
        ApiResponse expected = response(HttpStatus.BAD_REQUEST);
        when(communityManagementService.report("token", reportData)).thenReturn(expected);

        ResponseEntity<ApiResponse> result = controller.report(reportData, "token");

        assertSame(expected, result.getBody());
        assertEquals(HttpStatus.BAD_REQUEST, result.getStatusCode());
    }

    @Test
    void uploadFileDelegatesAndReturnsServiceStatus() {
        MockMultipartFile file = new MockMultipartFile("file", "test.txt", "text/plain", "data".getBytes());
        ApiResponse expected = response(HttpStatus.INTERNAL_SERVER_ERROR);
        when(communityManagementService.uploadFile(file, "community-1")).thenReturn(expected);

        ResponseEntity<ApiResponse> result = controller.uploadFile(file, "community-1");

        assertSame(expected, result.getBody());
        assertEquals(HttpStatus.INTERNAL_SERVER_ERROR, result.getStatusCode());
    }

    @Test
    void topicSearchDelegatesAndReturnsServiceStatus() {
        SearchCriteria criteria = new SearchCriteria();
        ApiResponse expected = response(HttpStatus.OK);
        when(communityManagementService.searchTopic(criteria)).thenReturn(expected);

        ResponseEntity<ApiResponse> result = controller.topicSearch(criteria);

        assertSame(expected, result.getBody());
        assertEquals(HttpStatus.OK, result.getStatusCode());
    }

    @Test
    void listAllCommunitiesJoinedByUserDelegatesAndAlwaysReturnsOk() {
        ApiResponse expected = response(HttpStatus.BAD_REQUEST);
        when(communityManagementService.listAllCommunitiesJoinedByUser("token")).thenReturn(expected);

        ResponseEntity<ApiResponse> result = controller.listAllCommunitiesJoinedByUser("token");

        assertSame(expected, result.getBody());
        assertEquals(HttpStatus.OK, result.getStatusCode());
    }

    @Test
    void publishDelegatesAndReturnsServiceStatus() {
        JsonNode details = mapper.createObjectNode();
        ApiResponse expected = response(HttpStatus.CONFLICT);
        when(communityManagementService.publish(details, "token")).thenReturn(expected);

        ResponseEntity<ApiResponse> result = controller.publish(details, "token");

        assertSame(expected, result.getBody());
        assertEquals(HttpStatus.CONFLICT, result.getStatusCode());
    }

    @Test
    void searchCommunityFromEsDelegatesAndReturnsServiceStatus() {
        SearchCriteria criteria = new SearchCriteria();
        ApiResponse expected = response(HttpStatus.OK);
        when(communityManagementService.searchCommunityFromPrimary(criteria)).thenReturn(expected);

        ResponseEntity<ApiResponse> result = controller.searchCommunityFromEs(criteria);

        assertSame(expected, result.getBody());
        assertEquals(HttpStatus.OK, result.getStatusCode());
    }

    @Test
    void adminReadCommunityDelegatesAndAlwaysReturnsOk() {
        ApiResponse expected = response(HttpStatus.BAD_REQUEST);
        when(communityManagementService.read("c1")).thenReturn(expected);

        ResponseEntity<ApiResponse> result = controller.adminReadCommunity("c1");

        assertSame(expected, result.getBody());
        assertEquals(HttpStatus.OK, result.getStatusCode());
    }

    @Test
    void syncUserWithCommunityDelegatesAndReturnsServiceStatus() {
        MockMultipartFile file = new MockMultipartFile("file", "users.csv", "text/csv", "data".getBytes());
        ApiResponse expected = response(HttpStatus.OK);
        when(communityManagementService.syncUserWithCommunity(file)).thenReturn(expected);

        ResponseEntity<ApiResponse> result = controller.searchCommunityFromEs(file);

        assertSame(expected, result.getBody());
        assertEquals(HttpStatus.OK, result.getStatusCode());
    }

    @Test
    void adminJoinDelegatesAndReturnsServiceStatus() {
        Map<String, Object> request = new HashMap<>();
        ApiResponse expected = response(HttpStatus.CONFLICT);
        when(communityManagementService.adminJoinCommunity(request, "token")).thenReturn(expected);

        ResponseEntity<ApiResponse> result = controller.adminJoin(request, "token");

        assertSame(expected, result.getBody());
        assertEquals(HttpStatus.CONFLICT, result.getStatusCode());
    }

    @Test
    void adminUnjoinDelegatesAndReturnsServiceStatus() {
        Map<String, Object> request = new HashMap<>();
        ApiResponse expected = response(HttpStatus.OK);
        when(communityManagementService.adminUnjoinCommunity(request, "token")).thenReturn(expected);

        ResponseEntity<ApiResponse> result = controller.adminUnjoin(request, "token");

        assertSame(expected, result.getBody());
        assertEquals(HttpStatus.OK, result.getStatusCode());
    }
}
