package com.igot.cb;

import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.springframework.boot.SpringApplication;
import org.springframework.http.client.HttpComponentsClientHttpRequestFactory;
import org.springframework.web.client.RestTemplate;

import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.mockStatic;

class CbCommunityServiceApplicationTest {

    @Test
    void mainDelegatesToSpringApplicationRun() {
        String[] args = new String[]{"--server.port=8080"};
        try (MockedStatic<SpringApplication> springApplication = mockStatic(SpringApplication.class)) {
            springApplication.when(() -> SpringApplication.run(CbCommunityServiceApplication.class, args))
                .thenReturn(null);

            CbCommunityServiceApplication.main(args);

            springApplication.verify(() -> SpringApplication.run(CbCommunityServiceApplication.class, args));
        }
    }

    @Test
    void restTemplateBeanIsCreatedWithPoolingHttpClientRequestFactory() {
        CbCommunityServiceApplication application = new CbCommunityServiceApplication();

        RestTemplate restTemplate = application.restTemplate();

        assertNotNull(restTemplate);
        assertInstanceOf(HttpComponentsClientHttpRequestFactory.class, restTemplate.getRequestFactory());
    }
}
