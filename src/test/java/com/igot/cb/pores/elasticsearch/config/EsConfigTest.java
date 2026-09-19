package com.igot.cb.pores.elasticsearch.config;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertNotNull;

class EsConfigTest {

    private EsConfig esConfig;

    @BeforeEach
    void setUp() {
        esConfig = new EsConfig();
        ReflectionTestUtils.setField(esConfig, "elasticsearchHost", "localhost");
        ReflectionTestUtils.setField(esConfig, "elasticsearchPort", 9200);
        ReflectionTestUtils.setField(esConfig, "elasticsearchUsername", "elastic");
        ReflectionTestUtils.setField(esConfig, "elasticsearchPassword", "changeme");
        ReflectionTestUtils.setField(esConfig, "userESClientHost", "localhost");
        ReflectionTestUtils.setField(esConfig, "userESClientPort", "9200");
    }

    @Test
    void elasticsearchClientIsCreatedWithConfiguredCredentials() throws IOException {
        ElasticsearchClient client = esConfig.elasticsearchClient();

        assertNotNull(client);
        client._transport().close();
    }

    @Test
    void userESClientIsCreatedForSingleHostAndPort() throws IOException {
        RestHighLevelClient client = esConfig.userESClient();

        assertNotNull(client);
        client.close();
    }

    @Test
    void userESClientIsCreatedForMultipleHostsAndPorts() throws IOException {
        ReflectionTestUtils.setField(esConfig, "userESClientHost", "host1,host2");
        ReflectionTestUtils.setField(esConfig, "userESClientPort", "9200,9201");

        RestHighLevelClient client = esConfig.userESClient();

        assertNotNull(client);
        client.close();
    }
}
