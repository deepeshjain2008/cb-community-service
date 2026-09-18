package com.igot.cb.pores.elasticsearch.service;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.FieldValue;
import co.elastic.clients.elasticsearch._types.Refresh;
import co.elastic.clients.elasticsearch._types.SortOrder;
import co.elastic.clients.elasticsearch._types.aggregations.Aggregate;
import co.elastic.clients.elasticsearch._types.aggregations.Aggregation;
import co.elastic.clients.elasticsearch._types.aggregations.StringTermsBucket;
import co.elastic.clients.elasticsearch._types.aggregations.TermsAggregation;
import co.elastic.clients.elasticsearch._types.query_dsl.*;
import co.elastic.clients.elasticsearch.core.*;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import co.elastic.clients.elasticsearch.core.bulk.BulkOperation;
import co.elastic.clients.elasticsearch.core.search.Hit;
import co.elastic.clients.elasticsearch.core.search.SourceConfig;
import co.elastic.clients.elasticsearch.indices.RefreshRequest;
import co.elastic.clients.json.JsonData;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.igot.cb.pores.elasticsearch.dto.FacetDTO;
import com.igot.cb.pores.elasticsearch.dto.SearchCriteria;
import com.igot.cb.pores.elasticsearch.dto.SearchResult;
import com.igot.cb.pores.exceptions.CustomException;
import com.igot.cb.pores.util.CbServerProperties;
import com.igot.cb.pores.util.Constants;
import com.networknt.schema.JsonSchemaFactory;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.MapUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


import org.elasticsearch.client.RequestOptions;


import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.io.InputStream;
import java.util.*;
import java.util.Map.Entry;

@Service
@Slf4j
public class EsUtilServiceImpl implements EsUtilService {

    private final ElasticsearchClient elasticsearchClient;
    private  final RestHighLevelClient userESClient;
    private final Logger logger = LogManager.getLogger(getClass());
    private final Map<String, Map<String, Object>> schemaCache = new ConcurrentHashMap<>();
    private static final String ERROR_CHECKING_COMMUNITY_EXISTENCE_LOG = "Error checking community existence in Elasticsearch: {}";


    private final ObjectMapper objectMapper;

    private final CbServerProperties cbServerProperties;

    @Autowired
    public EsUtilServiceImpl(@Qualifier("elasticsearchClient") ElasticsearchClient elasticsearchClient,
        @Qualifier("userESClient") RestHighLevelClient userESClient, ObjectMapper objectMapper,
        CbServerProperties cbServerProperties) {
        this.elasticsearchClient = elasticsearchClient;
        this.userESClient = userESClient;
        this.objectMapper = objectMapper;
        this.cbServerProperties = cbServerProperties;
    }

    @Value("${user_index_name}")
    private String userIndex;

    @Value("${community.index}")
    private String communityIndex;


    @Override
    public String addDocument(
            String esIndexName, String type, String id, Map<String, Object> document, String jsonFilePath) {
        logger.info("EsUtilServiceImpl :: addDocument");
        try {
            JsonSchemaFactory schemaFactory = JsonSchemaFactory.getInstance();
            InputStream schemaStream = schemaFactory.getClass().getResourceAsStream(jsonFilePath);
            Map<String, Object> map = objectMapper.readValue(schemaStream,
                new TypeReference<Map<String, Object>>() {
                });
            Iterator<Entry<String, Object>> iterator = document.entrySet().iterator();
            while (iterator.hasNext()) {
                Entry<String, Object> entry = iterator.next();
                String key = entry.getKey();
                if (!map.containsKey(key)) {
                    iterator.remove();
                }
            }
            IndexRequest<Map<String,Object>> indexRequest = new IndexRequest.Builder<Map<String, Object>>()
                .index(esIndexName)
                .id(id)
                .document(document)
                .refresh(Refresh.True)
                .build();
            IndexResponse response = elasticsearchClient.index(indexRequest);
            return "Successfully indexed document with id: " + response.result();
        } catch (Exception e) {
            logger.error("Issue while Indexing to es: {}", e.getMessage());
            return null;
        }
    }

    @Override
    public String updateDocument(
            String index, String indexType, String entityId, Map<String, Object> updatedDocument, String jsonFilePath) {
        try {
            JsonSchemaFactory schemaFactory = JsonSchemaFactory.getInstance();
            InputStream schemaStream = schemaFactory.getClass().getResourceAsStream(jsonFilePath);
            Map<String, Object> map = objectMapper.readValue(schemaStream,
                    new TypeReference<Map<String, Object>>() {
                    });
            Iterator<Entry<String, Object>> iterator = updatedDocument.entrySet().iterator();
            while (iterator.hasNext()) {
                Entry<String, Object> entry = iterator.next();
                String key = entry.getKey();
                if (!map.containsKey(key)) {
                    iterator.remove();
                }
            }
            IndexRequest<Map<String, Object>> indexRequest = new IndexRequest.Builder<Map<String, Object>>()
                .index(index)
                .id(entityId)
                .document(updatedDocument)
                .refresh(Refresh.True)
                .build();
            IndexResponse response = elasticsearchClient.index(indexRequest);
            return response.result().jsonValue();
        } catch (IOException e) {
            return null;
        }
    }

    @Override
    public void deleteDocument(String documentId, String esIndexName) {
        try {
            DeleteRequest request = new DeleteRequest.Builder().index(esIndexName).id(documentId).build();
            DeleteResponse response = elasticsearchClient.delete(request);
            if (response.result().jsonValue().equalsIgnoreCase("DELETED")) {
                log.info("Document deleted successfully from elasticsearch.");
                RefreshRequest refreshRequest = new RefreshRequest.Builder().index(esIndexName).build();
                elasticsearchClient.indices().refresh(refreshRequest);
                log.info("Index refreshed to reflect the document deletion.");
            } else {
                logger.error("Document not found or failed to delete from elasticsearch.");
            }
        } catch (Exception e) {
            logger.error("Error occurred during deleting document in elasticsearch");
        }
    }

    @Override
    public SearchResult searchDocuments(String esIndexName, SearchCriteria searchCriteria) {
        String searchString = searchCriteria.getSearchString();
        if (searchString != null && searchString.length() > cbServerProperties.getSearchStringMaxRegexLength()) {
            throw new CustomException(Constants.ERROR,
                "The length of the search string exceeds the allowed maximum of "
                    + cbServerProperties.getSearchStringMaxRegexLength() + " characters.",
                HttpStatus.BAD_REQUEST);
        }
        SearchRequest.Builder searchRequestBuilder = buildSearchRequest(searchCriteria);
        searchRequestBuilder.index(esIndexName);
        assert searchRequestBuilder != null;
        try {
            SearchResult searchResult = new SearchResult();

            if (searchCriteria != null) {
                int pageNumber = searchCriteria.getPageNumber();
                int pageSize = searchCriteria.getPageSize();
                int from = pageNumber * pageSize;
                searchRequestBuilder.from(from);
                if (pageSize > 0) {
                    searchRequestBuilder.size(pageSize);
                }
            }
            SearchRequest searchRequest = searchRequestBuilder.build();
            log.info("Final search query: {}", searchRequest.toString());
            SearchResponse<Object> paginatedSearchResponse =
                elasticsearchClient.search(searchRequest, Object.class);
            List<Map<String, Object>> paginatedResult = extractPaginatedResult(paginatedSearchResponse);
            Map<String, List<FacetDTO>> fieldAggregations =
                    extractFacetData(paginatedSearchResponse, searchCriteria);
            searchResult.setData(objectMapper.valueToTree(paginatedResult));
            searchResult.setFacets(fieldAggregations);
            searchResult.setTotalCount(paginatedSearchResponse.hits().total().value());
            return searchResult;
        } catch (IOException e) {
            logger.error("Error while fetching details from elastic search");
            return null;
        }
    }


    private Map<String, List<FacetDTO>> extractFacetData(
        SearchResponse<Object> searchResponse, SearchCriteria searchCriteria) {
        Map<String, List<FacetDTO>> fieldAggregations = new HashMap<>();
        if (searchCriteria.getFacets() != null) {
            for (String field : searchCriteria.getFacets()) {
                Aggregate aggregate = searchResponse
                    .aggregations()
                    .get(field + "_agg");
                if (aggregate.isSterms()) {
                    List<FacetDTO> fieldValueList = new ArrayList<>();
                    for (StringTermsBucket bucket : aggregate.sterms().buckets().array()) {
                        if (!bucket.key().stringValue().isEmpty()) {
                            FacetDTO facetDTO = new FacetDTO(bucket.key().stringValue(),
                                bucket.docCount());
                            fieldValueList.add(facetDTO);
                        }
                    }
                    fieldAggregations.put(field, fieldValueList);
                }
            }
        }
        return fieldAggregations;
    }

    private Map<String, List<FacetDTO>> extractFacetDataForList(
        SearchResponse<Object> searchResponse, SearchCriteria searchCriteria) {
        Map<String, List<FacetDTO>> fieldAggregations = new HashMap<>();
        if (searchCriteria.getFacets() != null) {
            for (String field : searchCriteria.getFacets()) {
                Aggregate aggregate = searchResponse
                    .aggregations()
                    .get(field + "_agg");

                if (aggregate.isSterms()) {
                    List<FacetDTO> fieldValueList = new ArrayList<>();
                    for (StringTermsBucket bucket : aggregate.sterms().buckets().array()) {
                        String key = bucket.key().stringValue();
                        long docCount = bucket.docCount();

                        FacetDTO facetDTO = new FacetDTO(key, docCount);
                        fieldValueList.add(facetDTO);
                    }

                    fieldAggregations.put(field, fieldValueList);
                }
            }
        }
        return fieldAggregations;
    }


    private List<Map<String, Object>> extractPaginatedResult(SearchResponse<Object> paginatedSearchResponse) {
        List<Map<String, Object>> paginatedResult = new ArrayList<>();

        // Process hits
        for (Hit<Object> hit : paginatedSearchResponse.hits().hits()) {
            paginatedResult.add((Map<String, Object>) hit.source());
        }

        appendTopHitsFromAggregations(paginatedSearchResponse.aggregations(), paginatedResult);

        return paginatedResult;
    }

    private void appendTopHitsFromAggregations(Map<String, Aggregate> aggregations,
            List<Map<String, Object>> paginatedResult) {
        if (aggregations == null || !aggregations.containsKey(Constants.TOPIC_ID)) {
            return;
        }
        Aggregate topicIdAgg = aggregations.get(Constants.TOPIC_ID);
        if (!topicIdAgg.isSterms()) {
            return;
        }
        for (StringTermsBucket bucket : topicIdAgg.sterms().buckets().array()) {
            appendTopHitsFromBucket(bucket, paginatedResult);
        }
    }

    private void appendTopHitsFromBucket(StringTermsBucket bucket, List<Map<String, Object>> paginatedResult) {
        Aggregate topHitsAgg = bucket.aggregations().get("top_hits#topNames");
        if (topHitsAgg == null || !topHitsAgg.isTopHits()) {
            return;
        }
        for (Hit<JsonData> hit : topHitsAgg.topHits().hits().hits()) {
            Map<String, Object> source = hit.source().to(Map.class); // Convert JsonData to Map
            paginatedResult.add(source);
        }
    }

    private SearchRequest.Builder buildSearchRequest(SearchCriteria searchCriteria) {
        logger.info("Building search query");
        if (searchCriteria == null || searchCriteria.toString().isEmpty()) {
            logger.error("Search criteria body is missing");
            return null;
        }
        BoolQuery.Builder boolQueryBuilder = buildFilterQuery(
            searchCriteria.getFilterCriteriaMap());
        if (boolQueryBuilder == null) {
            boolQueryBuilder = QueryBuilders.bool(); // Initialize an empty BoolQuery.Builder
        }
        // Add query string filter
        addQueryStringToFilter(searchCriteria.getSearchString(), boolQueryBuilder);
        // Add additional query parts
        Query queryPart = buildQueryPart(searchCriteria.getQuery());
        if (queryPart != null) {
            boolQueryBuilder.must(queryPart);
        }
        // Build the final query
        Query finalQuery = boolQueryBuilder.build()._toQuery();
        // Initialize the search request builder
        SearchRequest.Builder searchSourceBuilder = new SearchRequest.Builder();
        searchSourceBuilder.query(finalQuery);
        // Add sorting, requested fields, and facets
        addSortToSearchSourceBuilder(searchCriteria, searchSourceBuilder);
        addRequestedFieldsToSearchSourceBuilder(searchCriteria, searchSourceBuilder);
        addFacetsToSearchSourceBuilder(searchCriteria.getFacets(), searchSourceBuilder);
        return searchSourceBuilder;
    }


    private void addQueryStringToFilter(String searchString, BoolQuery.Builder boolQueryBuilder) {
        if (isNotBlank(searchString)) {
            List<String> fields = Arrays.asList(cbServerProperties.getSearchQueryFields().split(","));

            boolQueryBuilder.must(
                    Query.of(q -> q.multiMatch(m -> m
                            .fields(fields)
                            .query(searchString)))
            );
        }
    }


    private BoolQuery.Builder buildFilterQuery(Map<String, Object> filterCriteriaMap) {
        if (MapUtils.isEmpty(filterCriteriaMap)) {
            return null;
        }
        log.info("Search:: buildFilterQuery");
        BoolQuery.Builder boolQueryBuilder = QueryBuilders.bool();
        List<Query> mustNotQueries = new ArrayList<>();
        List<Query> boolQueries = new ArrayList<>();
        filterCriteriaMap.forEach((field, value) ->
                applyFilterEntry(field, value, boolQueryBuilder, mustNotQueries, boolQueries));
        mustNotQueries.forEach(boolQueryBuilder::mustNot);
        boolQueries.forEach(boolQueryBuilder::must);
        return boolQueryBuilder;
    }

    private void applyFilterEntry(String field, Object value, BoolQuery.Builder boolQueryBuilder,
            List<Query> mustNotQueries, List<Query> boolQueries) {
        if (field.equals("must_not") && value instanceof ArrayList<?> mustNotList) {
            @SuppressWarnings("unchecked")
            ArrayList<String> mustNotValues = (ArrayList<String>) mustNotList;
            mustNotQueries.add(Query.of(
                    q -> q.termsSet(t -> t.field(field).terms(mustNotValues))));
        } else if (value instanceof Boolean booleanValue) {
            boolQueries.add(
                    Query.of(q -> q.term(t -> t.field(field).value(booleanValue))));
        } else if (value instanceof ArrayList<?> rawList) {
            applyArrayListFilter(field, rawList, boolQueryBuilder);
        } else if (value instanceof String stringValue) {
            applyStringFilter(field, stringValue, boolQueryBuilder);
        } else if (value instanceof Map<?, ?> rawMap) {
            @SuppressWarnings("unchecked")
            Map<String, Object> nestedMap = (Map<String, Object>) rawMap;
            applyMapFilter(field, nestedMap, boolQueryBuilder);
        }
    }

    private void applyArrayListFilter(String field, ArrayList<?> rawList, BoolQuery.Builder boolQueryBuilder) {
        @SuppressWarnings("unchecked")
        ArrayList<String> valueList = (ArrayList<String>) rawList;
        List<FieldValue> termsList = valueList.stream()
                .map(FieldValue::of)
                .toList();
        boolQueryBuilder.must(Query.of(q -> q.terms(
                t -> t.field(field + Constants.KEYWORD)
                        .terms(terms -> terms.value(termsList)))));
    }

    private void applyStringFilter(String field, String stringValue, BoolQuery.Builder boolQueryBuilder) {
        boolQueryBuilder.must(Query.of(q -> q.terms(t ->
                t.field(field + Constants.KEYWORD)
                        .terms(terms -> terms.value(List.of(FieldValue.of(stringValue))))
        )));
    }

    private void applyMapFilter(String field, Map<String, Object> nestedMap, BoolQuery.Builder boolQueryBuilder) {
        if (isRangeQuery(nestedMap)) {
            applyRangeFilter(field, nestedMap, boolQueryBuilder);
        } else {
            nestedMap.forEach((nestedField, nestedValue) ->
                    applyNestedFieldFilter(field + "." + nestedField, nestedValue, boolQueryBuilder));
        }
    }

    private void applyRangeFilter(String field, Map<String, Object> nestedMap, BoolQuery.Builder boolQueryBuilder) {
        BoolQuery.Builder rangeOrNullQuery = QueryBuilders.bool();
        RangeQuery.Builder rangeQuery = QueryBuilders.range().field(field);
        nestedMap.forEach((rangeOperator, rangeValue) -> applyRangeOperator(rangeQuery, rangeOperator, rangeValue));
        rangeOrNullQuery.should(rangeQuery.build()._toQuery());
        rangeOrNullQuery.should(Query.of(q -> q.bool(
                b -> b.mustNot(Query.of(qn -> qn.exists(e -> e.field(field)))))));
        boolQueryBuilder.must(rangeOrNullQuery.build()._toQuery());
    }

    private void applyRangeOperator(RangeQuery.Builder rangeQuery, String rangeOperator, Object rangeValue) {
        switch (rangeOperator) {
            case Constants.SEARCH_OPERATION_GREATER_THAN_EQUALS:
                rangeQuery.gte(JsonData.of(rangeValue));
                break;
            case Constants.SEARCH_OPERATION_LESS_THAN_EQUALS:
                rangeQuery.lte(JsonData.of(rangeValue));
                break;
            case Constants.SEARCH_OPERATION_GREATER_THAN:
                rangeQuery.gt(JsonData.of(rangeValue));
                break;
            case Constants.SEARCH_OPERATION_LESS_THAN:
                rangeQuery.lt(JsonData.of(rangeValue));
                break;
            default:
                logger.warn("Unsupported range operator: {}", rangeOperator);
                break;
        }
    }

    private void applyNestedFieldFilter(String fullPath, Object nestedValue, BoolQuery.Builder boolQueryBuilder) {
        if (nestedValue instanceof Boolean booleanValue) {
            boolQueryBuilder.must(Query.of(q -> q.term(
                    t -> t.field(fullPath).value(booleanValue))));
        } else if (nestedValue instanceof String stringValue) {
            List<FieldValue> termList = Collections.singletonList(
                    FieldValue.of(stringValue));
            boolQueryBuilder.must(Query.of(q -> q.terms(
                    t -> t.field(fullPath + Constants.KEYWORD)
                            .terms((TermsQueryField) termList))));
        } else if (nestedValue instanceof ArrayList) {
            boolQueryBuilder.must(Query.of(q -> q.terms(
                    t -> t.field(fullPath + Constants.KEYWORD)
                            .terms((TermsQueryField) nestedValue))));
        }
    }

    private void addSortToSearchSourceBuilder(
            SearchCriteria searchCriteria, SearchRequest.Builder searchRequestBuilder) {
        if (searchCriteria == null ||
                !isNotBlank(searchCriteria.getOrderBy()) ||
                !isNotBlank(searchCriteria.getOrderDirection())) {
            return; // Nothing to sort, skip
        }
        if (isNotBlank(searchCriteria.getOrderBy()) && isNotBlank(searchCriteria.getOrderDirection())) {
            String sortField = searchCriteria.getOrderBy();
            Map<String, Object> schemaMap = readJsonSchema(cbServerProperties.getElasticCommunityJsonPath());
            Map<String, Object> fieldMap = (Map<String, Object>) schemaMap.get(sortField);

            if (MapUtils.isEmpty(fieldMap) ||
                    (!Constants.NUMBER.equals(fieldMap.get(Constants.TYPE)) && !Constants.LONG.equals(fieldMap.get(Constants.TYPE)) && !Constants.DATE.equals(fieldMap.get(Constants.TYPE)))) {
                sortField += Constants.KEYWORD;
            }

            String finalSortField = sortField;
            searchRequestBuilder.sort(s -> s.field(f -> f
                    .field(finalSortField)
                    .order(Constants.ASC.equalsIgnoreCase(searchCriteria.getOrderDirection()) ? SortOrder.Asc : SortOrder.Desc)
            ));
        }
    }


    private void addRequestedFieldsToSearchSourceBuilder(
        SearchCriteria searchCriteria, SearchRequest.Builder searchRequestBuilder) {
        if (searchCriteria.getRequestedFields() == null) {
            // Include all fields in the response
            searchRequestBuilder.source(SourceConfig.of(s -> s.fetch(true)));
        } else {
            if (searchCriteria.getRequestedFields().isEmpty()) {
                logger.error("Please specify at least one field to include in the results.");
            } else {
                // Include only the specified fields
                searchRequestBuilder.source(SourceConfig.of(s -> s.filter(f -> f.includes(searchCriteria.getRequestedFields()))));
            }
        }
    }


    private void addFacetsToSearchSourceBuilder(
        List<String> facets, SearchRequest.Builder searchRequestBuilder) {
        if (facets != null && !facets.isEmpty()) {
            Map<String, Aggregation> aggregationMap = facets.stream()
                .collect(Collectors.toMap(
                    field -> field + "_agg",
                    field -> Aggregation.of(a -> a.terms(
                        TermsAggregation.of(t -> t.field(field + Constants.KEYWORD).size(250))))
                ));
            searchRequestBuilder.aggregations(aggregationMap);
        }
    }

    private boolean isNotBlank(String value) {
        return value != null && !value.trim().isEmpty();
    }

    private boolean isRangeQuery(Map<String, Object> nestedMap) {
        return nestedMap.keySet().stream().anyMatch(key -> key.equals(Constants.SEARCH_OPERATION_GREATER_THAN_EQUALS) ||
                key.equals(Constants.SEARCH_OPERATION_LESS_THAN_EQUALS) || key.equals(Constants.SEARCH_OPERATION_GREATER_THAN) ||
                key.equals(Constants.SEARCH_OPERATION_LESS_THAN));
    }

    private Query buildQueryPart(Map<String, Object> queryMap) {
        log.info("Search:: buildQueryPart");
        if (queryMap == null || queryMap.isEmpty()) {
            return QueryBuilders.matchAll().build()._toQuery();
        }
        for (Entry<String, Object> entry : queryMap.entrySet()) {
            String key = entry.getKey();
            Object value = entry.getValue();

            switch (key) {
                case Constants.BOOL:
                    return buildBoolQuery((Map<String, Object>) value)._toQuery();
                case Constants.TERM:
                    return buildTermQuery((Map<String, Object>) value);
                case Constants.TERMS:
                    return buildTermsQuery((Map<String, Object>) value);
                case Constants.MATCH:
                    return buildMatchQuery((Map<String, Object>) value);
                case Constants.RANGE:
                    return buildRangeQuery((Map<String, Object>) value);
                default:
                    throw new IllegalArgumentException(Constants.UNSUPPORTED_QUERY + key);
            }
        }

        return null;
    }


    private BoolQuery buildBoolQuery(Map<String, Object> boolMap) {
        log.info("Search:: builderBoolQuery");
        BoolQuery.Builder boolQueryBuilder = QueryBuilders.bool();
        if (boolMap.containsKey(Constants.MUST)) {
            List<Map<String, Object>> mustList = (List<Map<String, Object>>) boolMap.get("must");
            mustList.forEach(must -> boolQueryBuilder.must(buildQueryPart(must)));
        }
        if (boolMap.containsKey(Constants.FILTER)) {
            List<Map<String, Object>> filterList = (List<Map<String, Object>>) boolMap.get("filter");
            filterList.forEach(filter -> boolQueryBuilder.filter(buildQueryPart(filter)));
        }
        if (boolMap.containsKey(Constants.MUST_NOT)) {
            List<Map<String, Object>> mustNotList = (List<Map<String, Object>>) boolMap.get("must_not");
            mustNotList.forEach(mustNot -> boolQueryBuilder.mustNot(buildQueryPart(mustNot)));
        }
        if (boolMap.containsKey(Constants.SHOULD)) {
            List<Map<String, Object>> shouldList = (List<Map<String, Object>>) boolMap.get("should");
            shouldList.forEach(should -> boolQueryBuilder.should(buildQueryPart(should)));
        }

        return boolQueryBuilder.build();
    }

    private Query buildTermQuery(Map<String, Object> termMap) {
        log.info("search::buildTermQuery");
        BoolQuery.Builder boolQueryBuilder = QueryBuilders.bool();
        for (Entry<String, Object> entry : termMap.entrySet()) {
            boolQueryBuilder.must(QueryBuilders.term(t -> t.field(entry.getKey()).value((FieldValue) entry.getValue())));
        }
        return boolQueryBuilder.build()._toQuery();
    }

    private Query buildTermsQuery(Map<String, Object> termsMap) {
        log.info("search:: buildTermsQuery");
        BoolQuery.Builder boolQueryBuilder = QueryBuilders.bool();
        for (Entry<String, Object> entry : termsMap.entrySet()) {
            boolQueryBuilder.must(QueryBuilders.terms(t -> t.field(entry.getKey()).terms((TermsQueryField) entry.getValue())));
        }
        return boolQueryBuilder.build()._toQuery();
    }

    private Query buildMatchQuery(Map<String, Object> matchMap) {
        log.info("search:: buildMatchQuery");
        BoolQuery.Builder boolQueryBuilder = QueryBuilders.bool();
        for (Entry<String, Object> entry : matchMap.entrySet()) {
            boolQueryBuilder.must(QueryBuilders.match(m -> m.field(entry.getKey()).query((FieldValue) entry.getValue())));
        }
        return boolQueryBuilder.build()._toQuery();
    }

    private Query buildRangeQuery(Map<String, Object> rangeMap) {
        log.info("search:: buildRangeQuery");
        BoolQuery.Builder boolQueryBuilder = QueryBuilders.bool();
        for (Entry<String, Object> entry : rangeMap.entrySet()) {
            Map<String, Object> rangeConditions = (Map<String, Object>) entry.getValue();
            RangeQuery.Builder rangeQueryBuilder = new RangeQuery.Builder().field(entry.getKey());
            rangeConditions.forEach((condition, value) -> {
                switch (condition) {
                    case "gt":
                        rangeQueryBuilder.gt(JsonData.of(value));
                        break;
                    case "gte":
                        rangeQueryBuilder.gte(JsonData.of(value));
                        break;
                    case "lt":
                        rangeQueryBuilder.lt(JsonData.of(value));
                        break;
                    case "lte":
                        rangeQueryBuilder.lte(JsonData.of(value));
                        break;
                    default:
                        throw new IllegalArgumentException(Constants.UNSUPPORTED_RANGE + condition);
                }
            });
            boolQueryBuilder.must(rangeQueryBuilder.build()._toQuery());
        }
        return boolQueryBuilder.build()._toQuery();
    }


    @Override
    public BulkResponse saveAll(String esIndexName, List<JsonNode> entities) throws IOException {
        try {
            log.info("EsUtilServiceImpl :: saveAll");
            List<BulkOperation> operations = new ArrayList<>();
            entities.forEach(entity -> {
                String formattedId = entity.get(Constants.ID).asText();
                Map<String, Object> entityMap = objectMapper.convertValue(entity, Map.class);
                BulkOperation operation = BulkOperation.of(b -> b
                    .index(i -> i
                        .index(esIndexName)
                        .id(formattedId)
                        .document(entityMap)
                    )
                );
                operations.add(operation);
            });

            BulkRequest bulkRequest = BulkRequest.of(b -> b.operations(operations));
            return elasticsearchClient.bulk(bulkRequest);
        } catch (Exception e) {
            log.error(e.getMessage());
            throw new CustomException("error bulk uploading", e.getMessage(),
                HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }


    @Override
    public SearchResult fetchTopCommunitiesForTopics(List<Integer> parentTopics, String indexName) throws IOException {
        logger.info("EsUtilService::fetchTopCommunitiesForTopics: inside method");

        try {
            // Create a terms query to filter documents based on parentTopics
            Query termsQuery = Query.of(q -> q.terms(t -> t
                .field(Constants.TOPIC_ID)
                .terms(terms -> terms.value(parentTopics.stream()
                    .map(FieldValue::of)
                    .toList()))
            ));

            // Create the terms aggregation
            Aggregation parentTopicsAgg = Aggregation.of(a -> a.terms(t -> t
                .field(Constants.TOPIC_ID)
                .size(parentTopics.size())
            ).aggregations("topNames", Aggregation.of(sub -> sub.topHits(th -> th.size(5)))));

            // Build the search request
            SearchRequest searchRequest = new SearchRequest.Builder()
                .index(indexName)
                .query(termsQuery)
                .size(0) // Do not return regular hits, only aggregations
                .aggregations(Constants.TOPIC_ID, parentTopicsAgg)
                .build();

            // Execute the search request
            SearchResponse<Object> response =
                elasticsearchClient.search(searchRequest, Object.class);

            // Extract aggregation results
            List<Map<String, Object>> paginatedResult = extractPaginatedResult(response);
            SearchCriteria searchCriteria = new SearchCriteria();
            List<String> facets = new ArrayList<>();
            facets.add(Constants.TOPIC_ID);
            searchCriteria.setFacets(facets);

            Map<String, List<FacetDTO>> fieldAggregations = extractFacetDataForList(response, searchCriteria);

            // Prepare the search result
            SearchResult searchResult = new SearchResult();
            searchResult.setData(objectMapper.valueToTree(paginatedResult));
            searchResult.setFacets(fieldAggregations);
            searchResult.setTotalCount(response.hits().total().value());
            return searchResult;

        } catch (Exception e) {
            logger.error("Error while fetching top communities for topics from Elasticsearch", e);
            throw new CustomException("Error while processing", e.getMessage(), HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    @Override
    public Boolean updateUserIndex(String userId, String communityId, Boolean append) {
        logger.info("EsUtilService::updateUserIndex:inside method");
        try {
            // Prepare parameters for the script
            // Prepare parameters for the script
            Map<String, Object> params = new HashMap<>();
            params.put("uuid", communityId);

            // Choose the script source based on the operation type.
            String scriptSource;
            if (Boolean.TRUE.equals(append)) {
                scriptSource = "if (ctx._source.containsKey('discussionCommunities') == false || ctx._source.discussionCommunities == null) {" +
                        "  ctx._source.discussionCommunities = [];" +
                        "} " +
                        "if (!ctx._source.discussionCommunities.contains(params.uuid)) {" +
                        "  ctx._source.discussionCommunities.add(params.uuid);" +
                        "}";
            } else {
                scriptSource = "if (ctx._source.containsKey('discussionCommunities') && ctx._source.discussionCommunities != null) {" +
                        "  ctx._source.discussionCommunities.removeIf(community -> community == params.uuid);" +
                        "}";
            }

// Prepare the script and upsert content
            Script script = new Script(ScriptType.INLINE, "painless", scriptSource, params);
            Map<String, Object> upsertContent = new HashMap<>();
            upsertContent.put(Constants.DISCUSSION_COMMUNITY_KEY, Collections.singletonList(communityId));

// Create the UpdateRequest (no type argument)
            UpdateRequest updateRequest = new UpdateRequest(userIndex, "_doc", userId)
                    .script(script)
                    .upsert(upsertContent)
                    .retryOnConflict(5);

// Log the request for debugging
            logger.info("UpdateRequest: {}", updateRequest);

// Execute the update
            UpdateResponse updateResponse = userESClient.update(updateRequest, RequestOptions.DEFAULT);

            DocWriteResponse.Result result = updateResponse.getResult();

            if (result == DocWriteResponse.Result.CREATED) {
                logger.info("WfRequests created successfully for userId: {}", userId);
            } else if (result == DocWriteResponse.Result.UPDATED) {
                logger.info("WfRequests updated successfully for userId: {}", userId);
            } else if (result == DocWriteResponse.Result.NOOP) {
                logger.info("WfRequests update was a noop; no changes were made for userId: {}", userId);
            } else {
                logger.warn("WfRequests update:: Unexpected result: {}, for userId: {}", result, userId);
            }

            return true; // Success

        } catch (ElasticsearchStatusException e) {
            if (e.status() == RestStatus.CONFLICT) {
                logger.warn("Conflict detected, retrying attempt  of ", e);
            } else {
                logger.error("Failed to upsert communityId for userId: {}", userId, e);
                return false;
            }
        } catch (Exception e) {
            logger.error("Failed to upsert communityId for userId: {}", userId, e);
            return false;
        }


        logger.error("Failed to upsert communityId for userId: {} after retries", userId);
        return false;
    }


    @Override
    public Boolean doesCommunityExist(String orgId, String communityName) {
        logger.info("EsUtilService::doesCommunityExist:inside method");
        try {
            // Build the exact match query
            Query query = Query.of(q -> q.bool(b -> b
                .must(m -> m.term(t -> t.field(Constants.ORG_ID + Constants.KEYWORD).value(orgId)))
                .must(m -> m.term(t -> t.field(Constants.COMMUNITY_NAME + Constants.KEYWORD).value(communityName)))
            ));

            // Create the search request
            SearchRequest searchRequest = new SearchRequest.Builder()
                .index(communityIndex)
                .query(query)
                .size(0) // We are only interested in the existence
                .build();

            // Execute the search
            SearchResponse<Object> searchResponse =
                elasticsearchClient.search(searchRequest, Object.class);

            // Check if any documents match the query
            return searchResponse.hits().total().value() > 0;
        } catch (Exception e) {
            log.error(ERROR_CHECKING_COMMUNITY_EXISTENCE_LOG, e.getMessage(), e);
            return false;
        }
    }

    @Override
    public boolean isDuplicateCommunity(String orgId, String communityName, String excludeCommunityId) {
        logger.info("EsUtilService::isDuplicateCommunity: inside method");

        try {
            // Build the query
            Query query = Query.of(q -> q.bool(b -> b
                .must(m -> m.term(t -> t.field(Constants.ORG_ID + Constants.KEYWORD).value(orgId)))
                .must(m -> m.term(t -> t.field(Constants.COMMUNITY_NAME + Constants.KEYWORD).value(communityName)))
                .mustNot(m -> {
                    if (excludeCommunityId != null && !excludeCommunityId.isEmpty()) {
                        return m.term(t -> t.field("_id").value(excludeCommunityId));
                    }
                    return null;
                })
            ));

            // Create the search request
            SearchRequest searchRequest = new SearchRequest.Builder()
                .index(communityIndex)
                .query(query)
                .size(0) // We are only interested in the existence
                .build();

            // Execute the search
            SearchResponse<Object> searchResponse =
                elasticsearchClient.search(searchRequest, Object.class);

            // Check if any documents match the query
            return searchResponse.hits().total().value() > 0;

        } catch (Exception e) {
            logger.error(ERROR_CHECKING_COMMUNITY_EXISTENCE_LOG, e.getMessage(), e);
            return false;
        }
    }


    @Override
    public Boolean doesCommunityNameExist(String communityName) {
        logger.info("EsUtilService::doesCommunityNameExist:inside method");
        try {
            // Build the exact match query
            Query query = Query.of(q -> q.term(t -> t.field(Constants.COMMUNITY_NAME + Constants.KEYWORD).value(communityName)));

            // Create the search request
            SearchRequest searchRequest = new SearchRequest.Builder()
                .index(communityIndex)
                .query(query)
                .size(0) // We are only interested in the existence
                .build();

            // Execute the search
            SearchResponse<Object> searchResponse =
                elasticsearchClient.search(searchRequest, Object.class);

            // Check if any documents match the query
            return searchResponse.hits().total().value() > 0;
        } catch (Exception e) {
            logger.error(ERROR_CHECKING_COMMUNITY_EXISTENCE_LOG, e.getMessage(), e);
            return false;
        }
    }

    @Override
    public Boolean doesCommunityNameExistForPublish(String communityName, String communityId) {
        logger.info("EsUtilService::doesCommunityNameExistForPublish:inside method");
        try {
            // Build the query
            Query query = Query.of(q -> q.bool(b -> b
                .must(m -> m.term(t -> t.field(Constants.COMMUNITY_NAME + Constants.KEYWORD).value(communityName)))
                .mustNot(m -> {
                    if (communityId != null && !communityId.isEmpty()) {
                        return m.term(t -> t.field("_id").value(communityId));
                    }
                    return null;
                })
            ));

            // Create the search request
            SearchRequest searchRequest = new SearchRequest.Builder()
                .index(communityIndex)
                .query(query)
                .size(0) // We are only interested in the existence
                .build();

            // Execute the search
            SearchResponse<Object> searchResponse =
                elasticsearchClient.search(searchRequest, Object.class);

            // Check if any documents match the query
            return searchResponse.hits().total().value() > 0;
        } catch (Exception e) {
            logger.error(ERROR_CHECKING_COMMUNITY_EXISTENCE_LOG, e.getMessage(), e);
            return false;
        }
    }

    @Override
    public SearchResponse popularCommunities(SearchRequest searchRequest, RequestOptions aDefault) {
        try {
            return elasticsearchClient.search(searchRequest, Object.class);
        } catch (Exception e) {
            logger.error("Error while fetching details from elastic search");
            return null;
        }
    }

    public  Map<String, Object> readJsonSchema(String jsonFilePath) {
        if (schemaCache.containsKey(jsonFilePath)) {
            return schemaCache.get(jsonFilePath);
        }

        try (InputStream schemaStream = JsonSchemaFactory.getInstance().getClass().getResourceAsStream(jsonFilePath)) {
            Map<String, Object> schemaMap = objectMapper.readValue(schemaStream, new TypeReference<Map<String, Object>>() {});
            schemaCache.put(jsonFilePath, schemaMap);
            return schemaMap;
        } catch (Exception e) {
            log.error("Error reading json schema", e);
            throw new CustomException("error reading json schema", e.getMessage(), HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

}

