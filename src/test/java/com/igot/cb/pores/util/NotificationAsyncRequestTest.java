package com.igot.cb.pores.util;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

class NotificationAsyncRequestTest {

    @Test
    void settersAndGettersRoundTrip() {
        NotificationAsyncRequest request = new NotificationAsyncRequest();
        Map<String, Object> action = new HashMap<>();
        action.put("k", "v");
        List<String> ids = List.of("id1", "id2");
        List<String> copyEmail = List.of("a@example.com");

        request.setType("email");
        request.setPriority(1);
        request.setAction(action);
        request.setIds(ids);
        request.setCopyEmail(copyEmail);

        assertEquals("email", request.getType());
        assertEquals(1, request.getPriority());
        assertEquals(action, request.getAction());
        assertEquals(ids, request.getIds());
        assertEquals(copyEmail, request.getCopyEmail());
    }
}
