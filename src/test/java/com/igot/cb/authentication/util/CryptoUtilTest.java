package com.igot.cb.authentication.util;

import org.junit.jupiter.api.Test;

import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.PublicKey;
import java.security.Signature;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CryptoUtilTest {

    @Test
    void verifyRSASignReturnsTrueForValidSignature() throws Exception {
        KeyPairGenerator generator = KeyPairGenerator.getInstance("RSA");
        generator.initialize(2048);
        KeyPair keyPair = generator.generateKeyPair();

        String payload = "hello world";
        Signature signer = Signature.getInstance("SHA256withRSA");
        signer.initSign(keyPair.getPrivate());
        signer.update(payload.getBytes(StandardCharsets.US_ASCII));
        byte[] signature = signer.sign();

        boolean result = CryptoUtil.verifyRSASign(payload, signature, keyPair.getPublic(), "SHA256withRSA");

        assertTrue(result);
    }

    @Test
    void verifyRSASignReturnsFalseForTamperedPayload() throws Exception {
        KeyPairGenerator generator = KeyPairGenerator.getInstance("RSA");
        generator.initialize(2048);
        KeyPair keyPair = generator.generateKeyPair();

        Signature signer = Signature.getInstance("SHA256withRSA");
        signer.initSign(keyPair.getPrivate());
        signer.update("original payload".getBytes(StandardCharsets.US_ASCII));
        byte[] signature = signer.sign();

        boolean result = CryptoUtil.verifyRSASign("tampered payload", signature, keyPair.getPublic(), "SHA256withRSA");

        assertFalse(result);
    }

    @Test
    void verifyRSASignReturnsFalseForUnknownAlgorithm() throws Exception {
        KeyPairGenerator generator = KeyPairGenerator.getInstance("RSA");
        generator.initialize(2048);
        PublicKey publicKey = generator.generateKeyPair().getPublic();

        boolean result = CryptoUtil.verifyRSASign("payload", new byte[]{1, 2, 3}, publicKey, "NotAnAlgorithm");

        assertFalse(result);
    }
}
