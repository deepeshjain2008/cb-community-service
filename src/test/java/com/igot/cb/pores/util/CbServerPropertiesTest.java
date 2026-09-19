package com.igot.cb.pores.util;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class CbServerPropertiesTest {

    @Test
    void settersAndGettersRoundTripForAllFields() {
        CbServerProperties props = new CbServerProperties();

        props.setSearchResultRedisTtl(100L);
        props.setSearchStringMaxRegexLength(50);
        props.setElasticCommunityJsonPath("/schema/community.json");
        props.setElasticCommunityCategoryJsonPath("/schema/category.json");
        props.setReporCommunityUserLimit(5);
        props.setDiscussionCloudFolderName("discussion");
        props.setDiscussionContainerName("container");
        props.setCloudStorageTypeName("aws");
        props.setCloudStorageKey("key");
        props.setCloudStorageSecret("secret");
        props.setCloudStorageEndpoint("endpoint");
        props.setCommunityModeratorTemplate("moderator-template");
        props.setSupportEmail("support@example.com");
        props.setNotifyServiceHost("http://notify");
        props.setNotifyServicePathAsync("/send");
        props.setDomainUrl("http://domain");
        props.setFixedCommunityUrl("/community");
        props.setSearchQueryFields("field1,field2");
        props.setRedisScanCountSize(10);
        props.setRedisCommunityUserDataTtlSeconds(200L);
        props.setCommunityAdminJoinMaxUser(20);
        props.setJwtSecretKey("jwt-secret");

        assertEquals(100L, props.getSearchResultRedisTtl());
        assertEquals(50, props.getSearchStringMaxRegexLength());
        assertEquals("/schema/community.json", props.getElasticCommunityJsonPath());
        assertEquals("/schema/category.json", props.getElasticCommunityCategoryJsonPath());
        assertEquals(5, props.getReporCommunityUserLimit());
        assertEquals("discussion", props.getDiscussionCloudFolderName());
        assertEquals("container", props.getDiscussionContainerName());
        assertEquals("aws", props.getCloudStorageTypeName());
        assertEquals("key", props.getCloudStorageKey());
        assertEquals("secret", props.getCloudStorageSecret());
        assertEquals("endpoint", props.getCloudStorageEndpoint());
        assertEquals("moderator-template", props.getCommunityModeratorTemplate());
        assertEquals("support@example.com", props.getSupportEmail());
        assertEquals("http://notify", props.getNotifyServiceHost());
        assertEquals("/send", props.getNotifyServicePathAsync());
        assertEquals("http://domain", props.getDomainUrl());
        assertEquals("/community", props.getFixedCommunityUrl());
        assertEquals("field1,field2", props.getSearchQueryFields());
        assertEquals(10, props.getRedisScanCountSize());
        assertEquals(200L, props.getRedisCommunityUserDataTtlSeconds());
        assertEquals(20, props.getCommunityAdminJoinMaxUser());
        assertEquals("jwt-secret", props.getJwtSecretKey());
    }
}
