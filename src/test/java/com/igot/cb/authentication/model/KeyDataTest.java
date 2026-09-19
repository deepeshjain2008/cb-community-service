package com.igot.cb.authentication.model;

import org.junit.jupiter.api.Test;

import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.PublicKey;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;

class KeyDataTest {

    @Test
    void constructorAndGettersRoundTrip() throws Exception {
        KeyPairGenerator generator = KeyPairGenerator.getInstance("RSA");
        generator.initialize(2048);
        KeyPair keyPair = generator.generateKeyPair();
        PublicKey publicKey = keyPair.getPublic();

        KeyData keyData = new KeyData("key-1", publicKey);

        assertEquals("key-1", keyData.getKeyId());
        assertSame(publicKey, keyData.getPublicKey());
    }

    @Test
    void settersUpdateFields() throws Exception {
        KeyPairGenerator generator = KeyPairGenerator.getInstance("RSA");
        generator.initialize(2048);
        PublicKey publicKey = generator.generateKeyPair().getPublic();
        KeyData keyData = new KeyData("key-1", null);

        keyData.setKeyId("key-2");
        keyData.setPublicKey(publicKey);

        assertEquals("key-2", keyData.getKeyId());
        assertSame(publicKey, keyData.getPublicKey());
    }
}
