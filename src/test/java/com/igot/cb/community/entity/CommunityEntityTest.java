package com.igot.cb.community.entity;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.sql.Timestamp;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CommunityEntityTest {

    @Test
    void allArgsConstructorAndGettersRoundTrip() throws Exception {
        JsonNode data = new ObjectMapper().readTree("{\"name\":\"community-1\"}");
        Timestamp createdOn = new Timestamp(System.currentTimeMillis());
        Timestamp updatedOn = new Timestamp(System.currentTimeMillis());

        CommunityEntity entity = new CommunityEntity("id-1", data, createdOn, updatedOn, "creator-1", true);

        assertEquals("id-1", entity.getCommunityId());
        assertEquals(data, entity.getData());
        assertEquals(createdOn, entity.getCreatedOn());
        assertEquals(updatedOn, entity.getUpdatedOn());
        assertEquals("creator-1", entity.getCreatedBy());
        assertTrue(entity.isActive());
    }

    @Test
    void noArgConstructorAndSettersRoundTrip() {
        CommunityEntity entity = new CommunityEntity();

        entity.setCommunityId("id-2");
        entity.setActive(false);

        assertEquals("id-2", entity.getCommunityId());
        assertFalse(entity.isActive());
    }
}
