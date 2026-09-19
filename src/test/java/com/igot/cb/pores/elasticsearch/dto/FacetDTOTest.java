package com.igot.cb.pores.elasticsearch.dto;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class FacetDTOTest {

    @Test
    void allArgsConstructorAndGettersRoundTrip() {
        FacetDTO dto = new FacetDTO("value-1", 5L);

        assertEquals("value-1", dto.getValue());
        assertEquals(5L, dto.getCount());
    }

    @Test
    void noArgConstructorAndSettersRoundTrip() {
        FacetDTO dto = new FacetDTO();

        dto.setValue("value-2");
        dto.setCount(10L);

        assertEquals("value-2", dto.getValue());
        assertEquals(10L, dto.getCount());
    }
}
