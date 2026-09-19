package com.igot.cb.pores.elasticsearch.dto;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

class SearchResultTest {

    @Test
    void allArgsConstructorAndGettersRoundTrip() throws Exception {
        JsonNode data = new ObjectMapper().readTree("[{\"id\":\"1\"}]");
        Map<String, List<FacetDTO>> facets = new HashMap<>();
        facets.put("category", List.of(new FacetDTO("value1", 3L)));
        List<Map<String, Object>> additionalInfo = new ArrayList<>();

        SearchResult result = new SearchResult(data, facets, 100L, additionalInfo);

        assertEquals(data, result.getData());
        assertEquals(facets, result.getFacets());
        assertEquals(100L, result.getTotalCount());
        assertEquals(additionalInfo, result.getAdditionalInfo());
    }

    @Test
    void noArgConstructorAndSettersRoundTrip() {
        SearchResult result = new SearchResult();

        result.setTotalCount(50L);

        assertEquals(50L, result.getTotalCount());
    }
}
