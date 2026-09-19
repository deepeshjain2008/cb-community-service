package com.igot.cb.pores.dto;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class RespParamTest {

    @Test
    void noArgConstructorLeavesFieldsNull() {
        RespParam param = new RespParam();

        assertNull(param.getResmsgid());
        assertNull(param.getStatus());
    }

    @Test
    void allArgsConstructorAndSettersRoundTrip() {
        RespParam param = new RespParam("res-1", "msg-1", "err-1", "SUCCESS", "errmsg-1");

        assertEquals("res-1", param.getResmsgid());
        assertEquals("msg-1", param.getMsgid());
        assertEquals("err-1", param.getErr());
        assertEquals("SUCCESS", param.getStatus());
        assertEquals("errmsg-1", param.getErrmsg());

        param.setResmsgid("res-2");
        param.setMsgid("msg-2");
        param.setErr("err-2");
        param.setStatus("FAILED");
        param.setErrmsg("errmsg-2");

        assertEquals("res-2", param.getResmsgid());
        assertEquals("msg-2", param.getMsgid());
        assertEquals("err-2", param.getErr());
        assertEquals("FAILED", param.getStatus());
        assertEquals("errmsg-2", param.getErrmsg());
    }
}
