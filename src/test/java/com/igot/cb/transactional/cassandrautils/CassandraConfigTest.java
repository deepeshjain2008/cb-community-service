package com.igot.cb.transactional.cassandrautils;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class CassandraConfigTest {

    private static class TestCassandraConfig extends CassandraConfig {
    }

    @Test
    void settersAndGettersRoundTrip() {
        TestCassandraConfig config = new TestCassandraConfig();

        config.setContactPoints("127.0.0.1");
        config.setPort(9042);
        config.setKeyspaceName("test_keyspace");

        assertEquals("127.0.0.1", config.getContactPoints());
        assertEquals(9042, config.getPort());
        assertEquals("test_keyspace", config.getKeyspaceName());
    }
}
