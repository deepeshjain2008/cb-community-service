package com.igot.cb.pores.elasticsearch.service;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.FieldValue;
import co.elastic.clients.elasticsearch._types.Result;
import co.elastic.clients.elasticsearch._types.aggregations.Aggregate;
import co.elastic.clients.elasticsearch._types.aggregations.Buckets;
import co.elastic.clients.elasticsearch._types.aggregations.StringTermsBucket;
import co.elastic.clients.elasticsearch._types.query_dsl.TermsQueryField;
import co.elastic.clients.elasticsearch.core.BulkResponse;
import co.elastic.clients.elasticsearch.core.DeleteResponse;
import co.elastic.clients.elasticsearch.core.IndexRequest;
import co.elastic.clients.elasticsearch.core.IndexResponse;
import co.elastic.clients.elasticsearch.core.SearchRequest;
import co.elastic.clients.elasticsearch.core.SearchResponse;
import co.elastic.clients.elasticsearch.core.search.Hit;
import co.elastic.clients.elasticsearch.core.search.TotalHitsRelation;
import co.elastic.clients.elasticsearch.indices.ElasticsearchIndicesClient;
import co.elastic.clients.json.JsonData;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.igot.cb.pores.elasticsearch.dto.SearchCriteria;
import com.igot.cb.pores.elasticsearch.dto.SearchResult;
import com.igot.cb.pores.exceptions.CustomException;
import com.igot.cb.pores.util.CbServerProperties;
import com.igot.cb.pores.util.Constants;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.rest.RestStatus;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.test.util.ReflectionTestUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class EsUtilServiceImplTest {

    private static final String COMMUNITY_INDEX = "community-index";
    private static final String USER_INDEX = "user-index";
    private static final String SCHEMA_PATH = "/EsFieldsmapping/esRequiredFieldsJsonFilePath.json";

    @Mock
    private ElasticsearchClient elasticsearchClient;

    @Mock
    private RestHighLevelClient userESClient;

    @Mock
    private ElasticsearchIndicesClient indicesClient;

    private EsUtilServiceImpl service;

    @BeforeEach
    void setUp() {
        CbServerProperties props = new CbServerProperties();
        props.setSearchStringMaxRegexLength(100);
        props.setSearchQueryFields("communityName,description");
        props.setElasticCommunityJsonPath(SCHEMA_PATH);

        service = new EsUtilServiceImpl(elasticsearchClient, userESClient, new ObjectMapper(), props);
        ReflectionTestUtils.setField(service, "userIndex", USER_INDEX);
        ReflectionTestUtils.setField(service, "communityIndex", COMMUNITY_INDEX);
    }

    // ---------- helpers to build real Elasticsearch response objects ----------

    private IndexResponse buildIndexResponse(Result result) {
        return IndexResponse.of(b -> b.index(COMMUNITY_INDEX).id("id1").primaryTerm(1)
            .result(result).seqNo(1).shards(s -> s.total(1).successful(1).failed(0)).version(1));
    }

    private DeleteResponse buildDeleteResponse(Result result) {
        return DeleteResponse.of(b -> b.index(COMMUNITY_INDEX).id("id1").primaryTerm(1)
            .result(result).seqNo(1).shards(s -> s.total(1).successful(1).failed(0)).version(1));
    }

    private BulkResponse buildBulkResponse() {
        return BulkResponse.of(b -> b.errors(false).took(1).items(Collections.emptyList()));
    }

    private Hit<Object> buildHit(Map<String, Object> source) {
        return Hit.of(h -> h.index(COMMUNITY_INDEX).id(java.util.UUID.randomUUID().toString()).source(source));
    }

    private SearchResponse<Object> buildSearchResponse(long total, List<Map<String, Object>> sources,
            Map<String, Aggregate> aggregations) {
        List<Hit<Object>> hits = new ArrayList<>();
        for (Map<String, Object> src : sources) {
            hits.add(buildHit(src));
        }
        return SearchResponse.of(b -> b
            .took(1)
            .timedOut(false)
            .shards(s -> s.total(1).successful(1).skipped(0).failed(0))
            .hits(h -> h.total(t -> t.value(total).relation(TotalHitsRelation.Eq)).hits(hits))
            .aggregations(aggregations == null ? Collections.emptyMap() : aggregations));
    }

    private Aggregate stringTermsAggregate(List<StringTermsBucket> buckets) {
        return Aggregate.of(a -> a.sterms(s -> s.buckets(Buckets.of(bb -> bb.array(buckets)))));
    }

    private SearchCriteria minimalCriteria() {
        SearchCriteria criteria = new SearchCriteria();
        criteria.setPageNumber(0);
        criteria.setPageSize(10);
        return criteria;
    }

    // ---------------------- addDocument ----------------------

    @Test
    void addDocumentReturnsSuccessMessageOnSuccess() throws IOException {
        when(elasticsearchClient.index(org.mockito.ArgumentMatchers.<IndexRequest<Map<String, Object>>>any()))
            .thenReturn(buildIndexResponse(Result.Created));

        Map<String, Object> document = new HashMap<>();
        document.put("communityName", "Test Community");
        document.put("unknownField", "should be stripped");

        String result = service.addDocument(COMMUNITY_INDEX, "_doc", "id1", document, SCHEMA_PATH);

        assertNotNull(result);
        assertTrue(result.contains("Created"));
    }

    @Test
    void addDocumentReturnsNullWhenSchemaPathInvalid() {
        Map<String, Object> document = new HashMap<>();
        document.put("communityName", "Test Community");

        String result = service.addDocument(COMMUNITY_INDEX, "_doc", "id1", document, "/does-not-exist.json");

        assertNull(result);
    }

    @Test
    void addDocumentReturnsNullOnClientException() throws IOException {
        when(elasticsearchClient.index(org.mockito.ArgumentMatchers.<IndexRequest<Map<String, Object>>>any()))
            .thenThrow(new RuntimeException("es down"));

        Map<String, Object> document = new HashMap<>();
        document.put("communityName", "Test Community");

        String result = service.addDocument(COMMUNITY_INDEX, "_doc", "id1", document, SCHEMA_PATH);

        assertNull(result);
    }

    // ---------------------- updateDocument ----------------------

    @Test
    void updateDocumentReturnsResultOnSuccess() throws IOException {
        when(elasticsearchClient.index(org.mockito.ArgumentMatchers.<IndexRequest<Map<String, Object>>>any()))
            .thenReturn(buildIndexResponse(Result.Updated));

        Map<String, Object> document = new HashMap<>();
        document.put("communityName", "Updated Community");

        String result = service.updateDocument(COMMUNITY_INDEX, "_doc", "id1", document, SCHEMA_PATH);

        assertEquals("updated", result);
    }

    @Test
    void updateDocumentThrowsForMissingSchemaResource() {
        // updateDocument only catches IOException, but a null schema stream makes
        // Jackson throw IllegalArgumentException, which propagates uncaught.
        Map<String, Object> document = new HashMap<>();
        document.put("communityName", "Updated Community");

        assertThrows(IllegalArgumentException.class, () ->
            service.updateDocument(COMMUNITY_INDEX, "_doc", "id1", document, "/does-not-exist.json"));
    }

    // ---------------------- deleteDocument ----------------------

    @Test
    void deleteDocumentRefreshesIndexOnSuccessfulDelete() throws IOException {
        when(elasticsearchClient.delete(any(co.elastic.clients.elasticsearch.core.DeleteRequest.class)))
            .thenReturn(buildDeleteResponse(Result.Deleted));
        when(elasticsearchClient.indices()).thenReturn(indicesClient);

        service.deleteDocument("id1", COMMUNITY_INDEX);

        verify(indicesClient).refresh(any(co.elastic.clients.elasticsearch.indices.RefreshRequest.class));
    }

    @Test
    void deleteDocumentDoesNotRefreshWhenNotDeleted() throws IOException {
        when(elasticsearchClient.delete(any(co.elastic.clients.elasticsearch.core.DeleteRequest.class)))
            .thenReturn(buildDeleteResponse(Result.NotFound));

        assertDoesNotThrow(() -> service.deleteDocument("id1", COMMUNITY_INDEX));

        verify(elasticsearchClient, never()).indices();
    }

    @Test
    void deleteDocumentHandlesException() throws IOException {
        when(elasticsearchClient.delete(any(co.elastic.clients.elasticsearch.core.DeleteRequest.class)))
            .thenThrow(new RuntimeException("es down"));

        assertDoesNotThrow(() -> service.deleteDocument("id1", COMMUNITY_INDEX));
    }

    // ---------------------- searchDocuments ----------------------

    @Test
    void searchDocumentsThrowsCustomExceptionWhenSearchStringTooLong() {
        SearchCriteria criteria = minimalCriteria();
        criteria.setSearchString("x".repeat(200));

        assertThrows(CustomException.class, () -> service.searchDocuments(COMMUNITY_INDEX, criteria));
    }

    @Test
    void searchDocumentsThrowsNpeForNullCriteria() {
        assertThrows(NullPointerException.class, () -> service.searchDocuments(COMMUNITY_INDEX, null));
    }

    @Test
    void searchDocumentsReturnsResultsForMinimalCriteria() throws IOException {
        SearchCriteria criteria = minimalCriteria();
        Map<String, Object> doc = new HashMap<>();
        doc.put("communityName", "Alpha");
        when(elasticsearchClient.search(any(SearchRequest.class), eq(Object.class)))
            .thenReturn(buildSearchResponse(1, List.of(doc), null));

        SearchResult result = service.searchDocuments(COMMUNITY_INDEX, criteria);

        assertEquals(1, result.getTotalCount());
    }

    @Test
    void searchDocumentsReturnsNullOnIOException() throws IOException {
        SearchCriteria criteria = minimalCriteria();
        when(elasticsearchClient.search(any(SearchRequest.class), eq(Object.class)))
            .thenThrow(new IOException("timeout"));

        SearchResult result = service.searchDocuments(COMMUNITY_INDEX, criteria);

        assertNull(result);
    }

    @Test
    void searchDocumentsAppliesFacetsSortAndRequestedFields() throws IOException {
        SearchCriteria criteria = minimalCriteria();
        criteria.setFacets(List.of("communityName"));
        criteria.setOrderBy("createdOn");
        criteria.setOrderDirection("asc");
        criteria.setRequestedFields(List.of("communityName"));

        Map<String, Aggregate> aggs = new HashMap<>();
        aggs.put("communityName_agg", stringTermsAggregate(List.of(
            StringTermsBucket.of(bk -> bk.key("General").docCount(5)),
            StringTermsBucket.of(bk -> bk.key("").docCount(2)))));
        when(elasticsearchClient.search(any(SearchRequest.class), eq(Object.class)))
            .thenReturn(buildSearchResponse(5, List.of(), aggs));

        SearchResult result = service.searchDocuments(COMMUNITY_INDEX, criteria);

        assertEquals(1, result.getFacets().get("communityName").size());
        assertEquals("General", result.getFacets().get("communityName").get(0).getValue());
    }

    @Test
    void searchDocumentsAppliesSortWithKeywordSuffixForTextField() throws IOException {
        SearchCriteria criteria = minimalCriteria();
        criteria.setOrderBy("communityName");
        criteria.setOrderDirection("desc");
        when(elasticsearchClient.search(any(SearchRequest.class), eq(Object.class)))
            .thenReturn(buildSearchResponse(0, List.of(), null));

        assertDoesNotThrow(() -> service.searchDocuments(COMMUNITY_INDEX, criteria));
    }

    @Test
    void searchDocumentsLogsErrorForEmptyRequestedFields() throws IOException {
        SearchCriteria criteria = minimalCriteria();
        criteria.setRequestedFields(new ArrayList<>());
        when(elasticsearchClient.search(any(SearchRequest.class), eq(Object.class)))
            .thenReturn(buildSearchResponse(0, List.of(), null));

        assertDoesNotThrow(() -> service.searchDocuments(COMMUNITY_INDEX, criteria));
    }

    @Test
    void searchDocumentsAppliesSearchStringAcrossQueryFields() throws IOException {
        SearchCriteria criteria = minimalCriteria();
        criteria.setSearchString("hello");
        when(elasticsearchClient.search(any(SearchRequest.class), eq(Object.class)))
            .thenReturn(buildSearchResponse(0, List.of(), null));

        assertDoesNotThrow(() -> service.searchDocuments(COMMUNITY_INDEX, criteria));
    }

    @Test
    void searchDocumentsAppliesAllFilterCriteriaVariations() throws IOException {
        SearchCriteria criteria = minimalCriteria();
        LinkedHashMap<String, Object> filters = new LinkedHashMap<>();
        filters.put("must_not", new ArrayList<>(List.of("excludedId")));
        filters.put("isActive", Boolean.TRUE);
        filters.put("tags", new ArrayList<>(List.of("tag1", "tag2")));
        filters.put("status", "live");
        Map<String, Object> nestedRange = new LinkedHashMap<>();
        nestedRange.put(Constants.SEARCH_OPERATION_GREATER_THAN_EQUALS, 1);
        nestedRange.put(Constants.SEARCH_OPERATION_LESS_THAN_EQUALS, 100);
        nestedRange.put(Constants.SEARCH_OPERATION_GREATER_THAN, 0);
        nestedRange.put(Constants.SEARCH_OPERATION_LESS_THAN, 200);
        filters.put("countOfPeopleJoined", nestedRange);
        Map<String, Object> nestedBoolean = new LinkedHashMap<>();
        nestedBoolean.put("flag", Boolean.FALSE);
        filters.put("nestedField", nestedBoolean);
        criteria.setFilterCriteriaMap(new java.util.HashMap<>(filters));
        when(elasticsearchClient.search(any(SearchRequest.class), eq(Object.class)))
            .thenReturn(buildSearchResponse(0, List.of(), null));

        assertDoesNotThrow(() -> service.searchDocuments(COMMUNITY_INDEX, criteria));
    }

    @Test
    void searchDocumentsThrowsClassCastExceptionForNestedStringFilter() {
        // Documents an existing production bug: applyNestedFieldFilter casts a
        // Collections.singletonList<FieldValue> to TermsQueryField, which always
        // throws ClassCastException for any nested String-valued filter.
        SearchCriteria criteria = minimalCriteria();
        Map<String, Object> nested = new HashMap<>();
        nested.put("label", "value");
        Map<String, Object> filters = new HashMap<>();
        filters.put("nestedField", nested);
        criteria.setFilterCriteriaMap(new java.util.HashMap<>(filters));

        assertThrows(ClassCastException.class, () -> service.searchDocuments(COMMUNITY_INDEX, criteria));
    }

    @Test
    void searchDocumentsRangeFilterHandlesUnsupportedOperator() throws IOException {
        SearchCriteria criteria = minimalCriteria();
        Map<String, Object> filters = new HashMap<>();
        Map<String, Object> rangeMap = new HashMap<>();
        rangeMap.put(Constants.SEARCH_OPERATION_GREATER_THAN_EQUALS, 1);
        rangeMap.put("eq", 5);
        filters.put("countOfPeopleJoined", rangeMap);
        criteria.setFilterCriteriaMap(new java.util.HashMap<>(filters));
        when(elasticsearchClient.search(any(SearchRequest.class), eq(Object.class)))
            .thenReturn(buildSearchResponse(0, List.of(), null));

        assertDoesNotThrow(() -> service.searchDocuments(COMMUNITY_INDEX, criteria));
    }

    @Test
    void searchDocumentsThrowsClassCastExceptionForNestedArrayListFilter() {
        SearchCriteria criteria = minimalCriteria();
        Map<String, Object> nested = new HashMap<>();
        nested.put("subfield", new ArrayList<>(List.of("a", "b")));
        Map<String, Object> filters = new HashMap<>();
        filters.put("category", nested);
        criteria.setFilterCriteriaMap(new java.util.HashMap<>(filters));

        assertThrows(ClassCastException.class, () -> service.searchDocuments(COMMUNITY_INDEX, criteria));
    }

    @Test
    void searchDocumentsQueryMapBoolTermMatchRange() throws IOException {
        SearchCriteria criteria = minimalCriteria();
        Map<String, Object> query = new HashMap<>();
        Map<String, Object> boolMap = new HashMap<>();
        boolMap.put("must", List.of(Map.of("term", Map.of("status", FieldValue.of("active")))));
        boolMap.put("filter", List.of(Map.of("match", Map.of("communityName", FieldValue.of("x")))));
        Map<String, Object> rangeConditions = new HashMap<>();
        rangeConditions.put("gt", 5);
        rangeConditions.put("gte", 5);
        rangeConditions.put("lt", 500);
        rangeConditions.put("lte", 500);
        boolMap.put("must_not", List.of(Map.of("range", Map.of("countOfPeopleJoined", rangeConditions))));
        boolMap.put("should", List.of(Map.of("term", Map.of("status", FieldValue.of("live")))));
        query.put("bool", boolMap);
        criteria.setQuery(query);
        when(elasticsearchClient.search(any(SearchRequest.class), eq(Object.class)))
            .thenReturn(buildSearchResponse(0, List.of(), null));

        assertDoesNotThrow(() -> service.searchDocuments(COMMUNITY_INDEX, criteria));
    }

    @Test
    void searchDocumentsQueryMapRangeThrowsForUnsupportedCondition() {
        SearchCriteria criteria = minimalCriteria();
        Map<String, Object> query = new HashMap<>();
        query.put("range", Map.of("countOfPeopleJoined", Map.of("unsupported", 1)));
        criteria.setQuery(query);

        assertThrows(IllegalArgumentException.class, () -> service.searchDocuments(COMMUNITY_INDEX, criteria));
    }

    @Test
    void searchDocumentsQueryMapTerms() throws IOException {
        SearchCriteria criteria = minimalCriteria();
        Map<String, Object> query = new HashMap<>();
        query.put("terms", Map.of("status", TermsQueryField.of(t -> t.value(List.of(FieldValue.of("active"))))));
        criteria.setQuery(query);
        when(elasticsearchClient.search(any(SearchRequest.class), eq(Object.class)))
            .thenReturn(buildSearchResponse(0, List.of(), null));

        assertDoesNotThrow(() -> service.searchDocuments(COMMUNITY_INDEX, criteria));
    }

    @Test
    void searchDocumentsQueryMapEmptyUsesMatchAll() throws IOException {
        SearchCriteria criteria = minimalCriteria();
        criteria.setQuery(new HashMap<>());
        when(elasticsearchClient.search(any(SearchRequest.class), eq(Object.class)))
            .thenReturn(buildSearchResponse(0, List.of(), null));

        assertDoesNotThrow(() -> service.searchDocuments(COMMUNITY_INDEX, criteria));
    }

    @Test
    void searchDocumentsQueryMapUnsupportedKeyThrows() {
        SearchCriteria criteria = minimalCriteria();
        Map<String, Object> query = new HashMap<>();
        query.put("unsupported_key", Map.of());
        criteria.setQuery(query);

        assertThrows(IllegalArgumentException.class, () -> service.searchDocuments(COMMUNITY_INDEX, criteria));
    }

    @Test
    void searchDocumentsAppendsTopHitsFromTopicAggregation() throws IOException {
        SearchCriteria criteria = minimalCriteria();
        JacksonJsonpMapper mapper = new JacksonJsonpMapper();
        Map<String, Object> topHitSource = new HashMap<>();
        topHitSource.put("communityName", "TopCommunity");
        Hit<JsonData> topHit = Hit.of(h -> h.index(COMMUNITY_INDEX).id("top1")
            .source(JsonData.of(topHitSource, mapper)));
        Aggregate topHitsAgg = Aggregate.of(a -> a.topHits(th -> th
            .hits(hm -> hm.total(t -> t.value(1).relation(TotalHitsRelation.Eq)).hits(List.of(topHit)))));
        StringTermsBucket bucketWithTopHits = StringTermsBucket.of(bk -> bk.key("topic1").docCount(1)
            .aggregations("top_hits#topNames", topHitsAgg));
        Map<String, Aggregate> topLevelAggs = new HashMap<>();
        topLevelAggs.put(Constants.TOPIC_ID, stringTermsAggregate(List.of(bucketWithTopHits)));

        when(elasticsearchClient.search(any(SearchRequest.class), eq(Object.class)))
            .thenReturn(buildSearchResponse(1, List.of(), topLevelAggs));

        SearchResult result = service.searchDocuments(COMMUNITY_INDEX, criteria);

        assertNotNull(result);
        JsonNode dataArray = result.getData();
        assertEquals(1, dataArray.size());
        assertEquals("TopCommunity", dataArray.get(0).get("communityName").asText());
    }

    @Test
    void searchDocumentsSkipsTopicAggregationWhenBucketHasNoTopHits() throws IOException {
        SearchCriteria criteria = minimalCriteria();
        StringTermsBucket bucketNoTopHits = StringTermsBucket.of(bk -> bk.key("topic1").docCount(1));
        Map<String, Aggregate> topLevelAggs = new HashMap<>();
        topLevelAggs.put(Constants.TOPIC_ID, stringTermsAggregate(List.of(bucketNoTopHits)));
        when(elasticsearchClient.search(any(SearchRequest.class), eq(Object.class)))
            .thenReturn(buildSearchResponse(0, List.of(), topLevelAggs));

        assertDoesNotThrow(() -> service.searchDocuments(COMMUNITY_INDEX, criteria));
    }

    @Test
    void searchDocumentsSkipsWhenNoAggregationsPresent() throws IOException {
        SearchCriteria criteria = minimalCriteria();
        when(elasticsearchClient.search(any(SearchRequest.class), eq(Object.class)))
            .thenReturn(buildSearchResponse(0, List.of(), Collections.emptyMap()));

        assertDoesNotThrow(() -> service.searchDocuments(COMMUNITY_INDEX, criteria));
    }

    // ---------------------- saveAll ----------------------

    @Test
    void saveAllReturnsBulkResponseOnSuccess() throws IOException {
        when(elasticsearchClient.bulk(any(co.elastic.clients.elasticsearch.core.BulkRequest.class)))
            .thenReturn(buildBulkResponse());
        ObjectMapper mapper = new ObjectMapper();
        JsonNode entity = mapper.createObjectNode().put(Constants.ID, "id1");

        BulkResponse response = service.saveAll(COMMUNITY_INDEX, List.of(entity));

        assertFalse(response.errors());
    }

    @Test
    void saveAllThrowsCustomExceptionOnFailure() throws IOException {
        when(elasticsearchClient.bulk(any(co.elastic.clients.elasticsearch.core.BulkRequest.class)))
            .thenThrow(new RuntimeException("bulk failed"));
        ObjectMapper mapper = new ObjectMapper();
        JsonNode entity = mapper.createObjectNode().put(Constants.ID, "id1");
        List<JsonNode> entities = List.of(entity);

        assertThrows(CustomException.class, () -> service.saveAll(COMMUNITY_INDEX, entities));
    }

    // ---------------------- fetchTopCommunitiesForTopics ----------------------

    @Test
    void fetchTopCommunitiesForTopicsReturnsResultOnSuccess() throws IOException {
        // extractFacetDataForList looks up the "topicId_agg" key, so the mocked
        // response must use that suffix for the extraction path to succeed.
        Map<String, Aggregate> aggs = new HashMap<>();
        aggs.put(Constants.TOPIC_ID + "_agg", stringTermsAggregate(List.of(
            StringTermsBucket.of(bk -> bk.key("1").docCount(3)))));
        when(elasticsearchClient.search(any(SearchRequest.class), eq(Object.class)))
            .thenReturn(buildSearchResponse(2, List.of(), aggs));

        SearchResult result = service.fetchTopCommunitiesForTopics(List.of(1, 2), COMMUNITY_INDEX);

        assertEquals(2, result.getTotalCount());
    }

    @Test
    void fetchTopCommunitiesForTopicsThrowsCustomExceptionWhenAggregationKeyMissing() throws IOException {
        // Documents an existing production bug: the request registers the
        // aggregation under key "topicId" (Constants.TOPIC_ID) but
        // extractFacetDataForList always looks up "topicId_agg", so a real
        // Elasticsearch response (keyed "topicId") triggers an NPE wrapped as
        // CustomException, every time.
        when(elasticsearchClient.search(any(SearchRequest.class), eq(Object.class)))
            .thenReturn(buildSearchResponse(2, List.of(), Collections.emptyMap()));
        List<Integer> topics = List.of(1, 2);

        assertThrows(CustomException.class, () -> service.fetchTopCommunitiesForTopics(topics, COMMUNITY_INDEX));
    }

    @Test
    void fetchTopCommunitiesForTopicsThrowsCustomExceptionOnFailure() throws IOException {
        when(elasticsearchClient.search(any(SearchRequest.class), eq(Object.class)))
            .thenThrow(new IOException("timeout"));
        List<Integer> topics = List.of(1, 2);

        assertThrows(CustomException.class, () -> service.fetchTopCommunitiesForTopics(topics, COMMUNITY_INDEX));
    }

    // ---------------------- updateUserIndex ----------------------

    private UpdateResponse mockUpdateResponse(DocWriteResponse.Result result) {
        UpdateResponse response = mock(UpdateResponse.class);
        lenient().when(response.getResult()).thenReturn(result);
        return response;
    }

    @Test
    void updateUserIndexReturnsTrueForCreatedResultWithAppend() throws IOException {
        UpdateResponse response = mockUpdateResponse(DocWriteResponse.Result.CREATED);
        when(userESClient.update(any(UpdateRequest.class), eq(RequestOptions.DEFAULT))).thenReturn(response);

        Boolean result = service.updateUserIndex("user1", "community1", true);

        assertTrue(result);
    }

    @Test
    void updateUserIndexReturnsTrueForUpdatedResultWithoutAppend() throws IOException {
        UpdateResponse response = mockUpdateResponse(DocWriteResponse.Result.UPDATED);
        when(userESClient.update(any(UpdateRequest.class), eq(RequestOptions.DEFAULT))).thenReturn(response);

        Boolean result = service.updateUserIndex("user1", "community1", false);

        assertTrue(result);
    }

    @Test
    void updateUserIndexReturnsTrueForNoopResult() throws IOException {
        UpdateResponse response = mockUpdateResponse(DocWriteResponse.Result.NOOP);
        when(userESClient.update(any(UpdateRequest.class), eq(RequestOptions.DEFAULT))).thenReturn(response);

        Boolean result = service.updateUserIndex("user1", "community1", true);

        assertTrue(result);
    }

    @Test
    void updateUserIndexReturnsTrueForUnexpectedResult() throws IOException {
        UpdateResponse response = mockUpdateResponse(DocWriteResponse.Result.NOT_FOUND);
        when(userESClient.update(any(UpdateRequest.class), eq(RequestOptions.DEFAULT))).thenReturn(response);

        Boolean result = service.updateUserIndex("user1", "community1", true);

        assertTrue(result);
    }

    @Test
    void updateUserIndexReturnsFalseOnConflictStatusException() throws IOException {
        when(userESClient.update(any(UpdateRequest.class), eq(RequestOptions.DEFAULT)))
            .thenThrow(new ElasticsearchStatusException("conflict", RestStatus.CONFLICT));

        Boolean result = service.updateUserIndex("user1", "community1", true);

        assertFalse(result);
    }

    @Test
    void updateUserIndexReturnsFalseOnNonConflictStatusException() throws IOException {
        when(userESClient.update(any(UpdateRequest.class), eq(RequestOptions.DEFAULT)))
            .thenThrow(new ElasticsearchStatusException("bad request", RestStatus.BAD_REQUEST));

        Boolean result = service.updateUserIndex("user1", "community1", true);

        assertFalse(result);
    }

    @Test
    void updateUserIndexReturnsFalseOnGenericException() throws IOException {
        when(userESClient.update(any(UpdateRequest.class), eq(RequestOptions.DEFAULT)))
            .thenThrow(new RuntimeException("boom"));

        Boolean result = service.updateUserIndex("user1", "community1", true);

        assertFalse(result);
    }

    // ---------------------- doesCommunityExist / isDuplicateCommunity / doesCommunityNameExist* ----------------------

    @Test
    void doesCommunityExistReturnsTrueWhenFound() throws IOException {
        when(elasticsearchClient.search(any(SearchRequest.class), eq(Object.class)))
            .thenReturn(buildSearchResponse(1, List.of(), Collections.emptyMap()));

        assertTrue(service.doesCommunityExist("org1", "Test Community"));
    }

    @Test
    void doesCommunityExistReturnsFalseWhenNotFound() throws IOException {
        when(elasticsearchClient.search(any(SearchRequest.class), eq(Object.class)))
            .thenReturn(buildSearchResponse(0, List.of(), Collections.emptyMap()));

        assertFalse(service.doesCommunityExist("org1", "Test Community"));
    }

    @Test
    void doesCommunityExistReturnsFalseOnException() throws IOException {
        when(elasticsearchClient.search(any(SearchRequest.class), eq(Object.class)))
            .thenThrow(new RuntimeException("es down"));

        assertFalse(service.doesCommunityExist("org1", "Test Community"));
    }

    @Test
    void isDuplicateCommunityReturnsTrueWithExcludeId() throws IOException {
        when(elasticsearchClient.search(any(SearchRequest.class), eq(Object.class)))
            .thenReturn(buildSearchResponse(1, List.of(), Collections.emptyMap()));

        assertTrue(service.isDuplicateCommunity("org1", "Test Community", "excludeId1"));
    }

    @Test
    void isDuplicateCommunityReturnsFalseWithoutExcludeId() {
        // A blank exclude-community-id triggers a latent bug: the must-not query
        // builder always dereferences a null result internally before Elasticsearch
        // is ever reached, and the surrounding exception handler swallows it and
        // returns false.
        assertFalse(service.isDuplicateCommunity("org1", "Test Community", null));
    }

    @Test
    void isDuplicateCommunityReturnsFalseOnException() throws IOException {
        when(elasticsearchClient.search(any(SearchRequest.class), eq(Object.class)))
            .thenThrow(new RuntimeException("es down"));

        assertFalse(service.isDuplicateCommunity("org1", "Test Community", "excludeId1"));
    }

    @Test
    void doesCommunityNameExistReturnsTrueWhenFound() throws IOException {
        when(elasticsearchClient.search(any(SearchRequest.class), eq(Object.class)))
            .thenReturn(buildSearchResponse(1, List.of(), Collections.emptyMap()));

        assertTrue(service.doesCommunityNameExist("Test Community"));
    }

    @Test
    void doesCommunityNameExistReturnsFalseOnException() throws IOException {
        when(elasticsearchClient.search(any(SearchRequest.class), eq(Object.class)))
            .thenThrow(new RuntimeException("es down"));

        assertFalse(service.doesCommunityNameExist("Test Community"));
    }

    @Test
    void doesCommunityNameExistForPublishReturnsTrueWithCommunityId() throws IOException {
        when(elasticsearchClient.search(any(SearchRequest.class), eq(Object.class)))
            .thenReturn(buildSearchResponse(1, List.of(), Collections.emptyMap()));

        assertTrue(service.doesCommunityNameExistForPublish("Test Community", "community1"));
    }

    @Test
    void doesCommunityNameExistForPublishReturnsFalseWithoutCommunityId() {
        // Same latent bug as isDuplicateCommunity: a blank community id trips the
        // same must-not query builder defect before Elasticsearch is ever called,
        // and the surrounding exception handler returns false.
        assertFalse(service.doesCommunityNameExistForPublish("Test Community", null));
    }

    @Test
    void doesCommunityNameExistForPublishReturnsFalseOnException() throws IOException {
        when(elasticsearchClient.search(any(SearchRequest.class), eq(Object.class)))
            .thenThrow(new RuntimeException("es down"));

        assertFalse(service.doesCommunityNameExistForPublish("Test Community", "community1"));
    }

    // ---------------------- popularCommunities ----------------------

    @Test
    void popularCommunitiesReturnsResponseOnSuccess() throws IOException {
        SearchRequest searchRequest = new SearchRequest.Builder().index(COMMUNITY_INDEX).build();
        when(elasticsearchClient.search(searchRequest, Object.class))
            .thenReturn(buildSearchResponse(0, List.of(), null));

        SearchResponse<Object> response = service.popularCommunities(searchRequest, RequestOptions.DEFAULT);

        assertNotNull(response);
    }

    @Test
    void popularCommunitiesReturnsNullOnException() throws IOException {
        SearchRequest searchRequest = new SearchRequest.Builder().index(COMMUNITY_INDEX).build();
        when(elasticsearchClient.search(searchRequest, Object.class))
            .thenThrow(new RuntimeException("es down"));

        SearchResponse<Object> response = service.popularCommunities(searchRequest, RequestOptions.DEFAULT);

        assertNull(response);
    }

    // ---------------------- readJsonSchema ----------------------

    @Test
    void readJsonSchemaCachesResultOnSecondCall() {
        Map<String, Object> first = service.readJsonSchema(SCHEMA_PATH);
        Map<String, Object> second = service.readJsonSchema(SCHEMA_PATH);

        assertNotNull(first);
        assertEquals(first, second);
    }

    @Test
    void readJsonSchemaThrowsCustomExceptionForMissingResource() {
        assertThrows(CustomException.class, () -> service.readJsonSchema("/does-not-exist.json"));
    }
}
