package com.igot.cb.community.entity;

import org.junit.jupiter.api.Test;

import java.sql.Timestamp;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CommunityCategoryTest {

    @Test
    void defaultIsActiveIsTrue() {
        CommunityCategory category = new CommunityCategory();

        assertTrue(category.getIsActive());
    }

    @Test
    void allArgsConstructorAndGettersRoundTrip() {
        Timestamp createdAt = new Timestamp(System.currentTimeMillis());
        Timestamp updatedAt = new Timestamp(System.currentTimeMillis());

        CommunityCategory category = new CommunityCategory(
            1, "Category1", "description", null, false, createdAt, updatedAt, "dept-1", 5L);

        assertEquals(1, category.getCategoryId());
        assertEquals("Category1", category.getCategoryName());
        assertEquals("description", category.getDescription());
        assertEquals(false, category.getIsActive());
        assertEquals(createdAt, category.getCreatedAt());
        assertEquals(updatedAt, category.getLastUpdatedAt());
        assertEquals("dept-1", category.getDepartmentId());
        assertEquals(5L, category.getCountOfCommunities());
    }

    @Test
    void settersUpdateFields() {
        CommunityCategory category = new CommunityCategory();

        category.setCategoryId(2);
        category.setCategoryName("Category2");
        category.setParentId(1);
        category.setIsActive(false);

        assertEquals(2, category.getCategoryId());
        assertEquals("Category2", category.getCategoryName());
        assertEquals(1, category.getParentId());
        assertEquals(false, category.getIsActive());
    }
}
