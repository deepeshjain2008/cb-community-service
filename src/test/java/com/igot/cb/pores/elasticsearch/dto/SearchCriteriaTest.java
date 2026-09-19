package com.igot.cb.pores.elasticsearch.dto;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SearchCriteriaTest {

    @Test
    void allArgsConstructorAndGettersRoundTrip() {
        HashMap<String, Object> filterMap = new HashMap<>();
        filterMap.put("status", "active");
        List<String> requestedFields = List.of("field1");
        List<String> facets = List.of("facet1");
        Map<String, Object> query = new HashMap<>();
        query.put("match", "value");

        SearchCriteria criteria = new SearchCriteria(
            filterMap, requestedFields, 1, 10, "name", "asc", "search text", facets, query, true);

        assertEquals(filterMap, criteria.getFilterCriteriaMap());
        assertEquals(requestedFields, criteria.getRequestedFields());
        assertEquals(1, criteria.getPageNumber());
        assertEquals(10, criteria.getPageSize());
        assertEquals("name", criteria.getOrderBy());
        assertEquals("asc", criteria.getOrderDirection());
        assertEquals("search text", criteria.getSearchString());
        assertEquals(facets, criteria.getFacets());
        assertEquals(query, criteria.getQuery());
        assertTrue(criteria.isOverrideCache());
    }

    @Test
    void noArgConstructorAndSettersRoundTrip() {
        SearchCriteria criteria = new SearchCriteria();

        criteria.setPageNumber(2);
        criteria.setPageSize(20);
        criteria.setOverrideCache(false);

        assertEquals(2, criteria.getPageNumber());
        assertEquals(20, criteria.getPageSize());
        assertEquals(false, criteria.isOverrideCache());
    }
}
