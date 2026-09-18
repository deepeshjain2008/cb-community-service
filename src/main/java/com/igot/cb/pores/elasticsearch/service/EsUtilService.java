package com.igot.cb.pores.elasticsearch.service;

import co.elastic.clients.elasticsearch.core.SearchRequest;
import co.elastic.clients.elasticsearch.core.SearchResponse;
import com.fasterxml.jackson.databind.JsonNode;
import com.igot.cb.pores.elasticsearch.dto.SearchCriteria;
import com.igot.cb.pores.elasticsearch.dto.SearchResult;
import co.elastic.clients.elasticsearch.core.BulkResponse;

import org.elasticsearch.client.RequestOptions;


import java.io.IOException;
import java.util.List;
import java.util.Map;

public interface EsUtilService {
  String addDocument(String esIndexName, String type, String id, Map<String, Object> document, String jsonFilePath);

  String updateDocument(String index, String indexType, String entityId, Map<String, Object> document, String jsonFilePath);

  void deleteDocument(String documentId, String esIndexName);


  SearchResult searchDocuments(String esIndexName, SearchCriteria searchCriteria);


  public BulkResponse saveAll(String esIndexName, List<JsonNode> entities) throws IOException;

  SearchResult fetchTopCommunitiesForTopics(List<Integer> topicIds, String indexName) throws IOException;


  SearchResponse popularCommunities(SearchRequest searchRequest, RequestOptions aDefault);

  Boolean updateUserIndex (String userId, String communityId, Boolean append);

  public Boolean doesCommunityExist(String orgId, String communityName);

  boolean isDuplicateCommunity(String orgId, String communityName, String excludeCommunityId);

  public Boolean doesCommunityNameExist(String communityName);

  public Boolean doesCommunityNameExistForPublish(String communityName, String communityId);
}
