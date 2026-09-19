package com.igot.cb.pores.util;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class ApiRespParamTest {

    @Test
    void noArgConstructorLeavesFieldsNull() {
        ApiRespParam param = new ApiRespParam();

        assertNull(param.getResMsgId());
        assertNull(param.getMsgId());
    }

    @Test
    void idConstructorSetsResMsgIdAndMsgId() {
        ApiRespParam param = new ApiRespParam("id-1");

        assertEquals("id-1", param.getResMsgId());
        assertEquals("id-1", param.getMsgId());
    }

    @Test
    void settersAndGettersRoundTrip() {
        ApiRespParam param = new ApiRespParam();

        param.setResMsgId("res-1");
        param.setMsgId("msg-1");
        param.setErr("err-1");
        param.setStatus("FAILED");
        param.setErrMsg("something broke");

        assertEquals("res-1", param.getResMsgId());
        assertEquals("msg-1", param.getMsgId());
        assertEquals("err-1", param.getErr());
        assertEquals("FAILED", param.getStatus());
        assertEquals("something broke", param.getErrMsg());
    }
}
