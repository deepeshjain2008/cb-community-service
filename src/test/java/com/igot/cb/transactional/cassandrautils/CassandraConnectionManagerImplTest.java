package com.igot.cb.transactional.cassandrautils;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.metadata.EndPoint;
import com.datastax.oss.driver.api.core.metadata.Metadata;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.igot.cb.pores.exceptions.CustomException;
import com.igot.cb.pores.util.Constants;
import com.igot.cb.pores.util.PropertiesCache;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.MockedStatic;

import java.lang.reflect.Field;
import java.util.*;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class CassandraConnectionManagerImplTest {

    private String originalHost;
    private String originalConsistency;

    @BeforeEach
    void setUp() throws Exception {
        originalHost = getProperty(Constants.CASSANDRA_CONFIG_HOST);
        originalConsistency = getProperty(Constants.SUNBIRD_CASSANDRA_CONSISTENCY_LEVEL);
        sessionMap().clear();
        setStaticSession(null);
    }

    @AfterEach
    void tearDown() throws Exception {
        setProperty(Constants.CASSANDRA_CONFIG_HOST, originalHost);
        setProperty(Constants.SUNBIRD_CASSANDRA_CONSISTENCY_LEVEL, originalConsistency);
        sessionMap().clear();
        setStaticSession(null);
    }

    private String getProperty(String key) throws Exception {
        Properties props = configProp();
        return props.getProperty(key);
    }

    private void setProperty(String key, String value) throws Exception {
        Properties props = configProp();
        if (value == null) {
            props.remove(key);
        } else {
            props.setProperty(key, value);
        }
    }

    private Properties configProp() throws Exception {
        PropertiesCache cache = PropertiesCache.getInstance();
        Field field = PropertiesCache.class.getDeclaredField("configProp");
        field.setAccessible(true);
        return (Properties) field.get(cache);
    }

    @SuppressWarnings("unchecked")
    private Map<String, CqlSession> sessionMap() throws Exception {
        Field field = CassandraConnectionManagerImpl.class.getDeclaredField("cassandraSessionMap");
        field.setAccessible(true);
        return (Map<String, CqlSession>) field.get(null);
    }

    private void setStaticSession(CqlSession session) throws Exception {
        Field field = CassandraConnectionManagerImpl.class.getDeclaredField("session");
        field.setAccessible(true);
        field.set(null, session);
    }

    private CqlSession newMockSessionWithMetadata() {
        CqlSession session = mock(CqlSession.class);
        Metadata metadata = mock(Metadata.class);
        Node node = mock(Node.class);
        EndPoint endPoint = mock(EndPoint.class);
        when(node.getDatacenter()).thenReturn("datacenter1");
        when(node.getRack()).thenReturn("rack1");
        when(node.getEndPoint()).thenReturn(endPoint);
        Map<UUID, Node> nodes = new HashMap<>();
        nodes.put(UUID.randomUUID(), node);
        when(metadata.getNodes()).thenReturn(nodes);
        when(metadata.getClusterName()).thenReturn(Optional.of("test-cluster"));
        when(session.getMetadata()).thenReturn(metadata);
        return session;
    }

    private CqlSessionBuilder mockedBuilder(CqlSession sessionToReturn) {
        CqlSessionBuilder builder = mock(CqlSessionBuilder.class);
        when(builder.addContactPoints(anyCollection())).thenReturn(builder);
        when(builder.withLocalDatacenter(anyString())).thenReturn(builder);
        when(builder.withKeyspace(anyString())).thenReturn(builder);
        when(builder.withConfigLoader(any(DriverConfigLoader.class))).thenReturn(builder);
        when(builder.build()).thenReturn(sessionToReturn);
        return builder;
    }

    @Test
    void constructorThrowsCustomExceptionWhenHostBlank() throws Exception {
        setProperty(Constants.CASSANDRA_CONFIG_HOST, "");

        assertThrows(CustomException.class, CassandraConnectionManagerImpl::new);
    }

    @Test
    void constructorSucceedsWithMockedCqlSessionBuilder() throws Exception {
        setProperty(Constants.CASSANDRA_CONFIG_HOST, "localhost");
        CqlSession rootSession = newMockSessionWithMetadata();

        CqlSessionBuilder builder = mockedBuilder(rootSession);
        try (MockedStatic<CqlSession> mockedStatic = mockStatic(CqlSession.class)) {
            mockedStatic.when(CqlSession::builder).thenReturn(builder);

            assertDoesNotThrow(CassandraConnectionManagerImpl::new);
        }
    }

    @Test
    void constructorWrapsBuilderFailureAsCustomException() throws Exception {
        setProperty(Constants.CASSANDRA_CONFIG_HOST, "localhost");

        try (MockedStatic<CqlSession> mockedStatic = mockStatic(CqlSession.class)) {
            mockedStatic.when(CqlSession::builder).thenThrow(new IllegalStateException("cannot connect"));

            assertThrows(CustomException.class, CassandraConnectionManagerImpl::new);
        }
    }

    @Test
    void getSessionReturnsCachedSessionWhenNotClosed() throws Exception {
        CqlSession cached = mock(CqlSession.class);
        when(cached.isClosed()).thenReturn(false);
        sessionMap().put("ks1", cached);

        CassandraConnectionManagerImpl manager =
            mock(CassandraConnectionManagerImpl.class, CALLS_REAL_METHODS);

        CqlSession result = manager.getSession("ks1");

        assertSame(cached, result);
    }

    @Test
    void getSessionCreatesNewSessionWhenCachedSessionIsClosed() throws Exception {
        setProperty(Constants.CASSANDRA_CONFIG_HOST, "localhost");
        CqlSession closed = mock(CqlSession.class);
        when(closed.isClosed()).thenReturn(true);
        sessionMap().put("ks2", closed);
        CqlSession freshSession = newMockSessionWithMetadata();

        CassandraConnectionManagerImpl manager =
            mock(CassandraConnectionManagerImpl.class, CALLS_REAL_METHODS);

        CqlSessionBuilder builder = mockedBuilder(freshSession);
        try (MockedStatic<CqlSession> mockedStatic = mockStatic(CqlSession.class)) {
            mockedStatic.when(CqlSession::builder).thenReturn(builder);

            CqlSession result = manager.getSession("ks2");

            assertNotSame(closed, result);
            assertSame(freshSession, result);
            assertSame(freshSession, sessionMap().get("ks2"));
        }
    }

    @Test
    void getSessionCreatesNewSessionWhenNotCached() throws Exception {
        setProperty(Constants.CASSANDRA_CONFIG_HOST, "localhost");
        CqlSession freshSession = newMockSessionWithMetadata();

        CassandraConnectionManagerImpl manager =
            mock(CassandraConnectionManagerImpl.class, CALLS_REAL_METHODS);

        CqlSessionBuilder builder = mockedBuilder(freshSession);
        try (MockedStatic<CqlSession> mockedStatic = mockStatic(CqlSession.class)) {
            mockedStatic.when(CqlSession::builder).thenReturn(builder);

            CqlSession result = manager.getSession("ks3");

            assertNotNull(result);
            assertSame(freshSession, sessionMap().get("ks3"));
        }
    }

    @Test
    void getSessionThrowsCustomExceptionWhenHostBlank() throws Exception {
        setProperty(Constants.CASSANDRA_CONFIG_HOST, "   ");

        CassandraConnectionManagerImpl manager =
            mock(CassandraConnectionManagerImpl.class, CALLS_REAL_METHODS);

        assertThrows(CustomException.class, () -> manager.getSession("ks4"));
    }

    @ParameterizedTest(name = "getConsistencyLevelResolves_{0}")
    @MethodSource("consistencyLevelScenarios")
    void getConsistencyLevelResolvesConfiguredValue(String scenario, String configuredValue, String expected) throws Exception {
        setProperty(Constants.SUNBIRD_CASSANDRA_CONSISTENCY_LEVEL, configuredValue);

        Object level = invokeGetConsistencyLevel();

        assertEquals(expected, level.toString());
    }

    private static Stream<Arguments> consistencyLevelScenarios() {
        return Stream.of(
            Arguments.of("blank", "", "LOCAL_ONE"),
            Arguments.of("configuredLevel", "quorum", "QUORUM"),
            Arguments.of("invalidValue", "not-a-real-level", "LOCAL_ONE")
        );
    }

    private Object invokeGetConsistencyLevel() throws Exception {
        java.lang.reflect.Method method =
            CassandraConnectionManagerImpl.class.getDeclaredMethod("getConsistencyLevel");
        method.setAccessible(true);
        return method.invoke(null);
    }

    @Test
    void registerShutdownHookDoesNotThrow() {
        assertDoesNotThrow(CassandraConnectionManagerImpl::registerShutdownHook);
    }

    @Test
    void resourceCleanUpClosesAllSessionsAndStaticSession() throws Exception {
        CqlSession hashedSession = mock(CqlSession.class);
        sessionMap().put("ks5", hashedSession);
        CqlSession staticSession = mock(CqlSession.class);
        setStaticSession(staticSession);

        CassandraConnectionManagerImpl.ResourceCleanUp cleanUp =
            new CassandraConnectionManagerImpl.ResourceCleanUp();

        assertDoesNotThrow(cleanUp::run);
    }

    @Test
    void resourceCleanUpSkipsStaticSessionWhenNull() throws Exception {
        CqlSession hashedSession = mock(CqlSession.class);
        sessionMap().put("ks6", hashedSession);
        setStaticSession(null);

        CassandraConnectionManagerImpl.ResourceCleanUp cleanUp =
            new CassandraConnectionManagerImpl.ResourceCleanUp();

        assertDoesNotThrow(cleanUp::run);
    }

    @Test
    void resourceCleanUpSwallowsExceptionFromSessionClose() throws Exception {
        CqlSession failingSession = mock(CqlSession.class);
        org.mockito.Mockito.doThrow(new RuntimeException("close failed")).when(failingSession).close();
        sessionMap().put("ks7", failingSession);

        CassandraConnectionManagerImpl.ResourceCleanUp cleanUp =
            new CassandraConnectionManagerImpl.ResourceCleanUp();

        assertDoesNotThrow(cleanUp::run);
    }
}
