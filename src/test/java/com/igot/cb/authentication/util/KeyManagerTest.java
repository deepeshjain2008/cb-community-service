package com.igot.cb.authentication.util;

import com.igot.cb.authentication.model.KeyData;
import com.igot.cb.pores.util.Constants;
import com.igot.cb.pores.util.PropertiesCache;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.PublicKey;
import java.util.Base64;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

class KeyManagerTest {

    @TempDir
    Path tempDir;

    private KeyManager keyManager = new KeyManager();

    @AfterEach
    void resetBasePath() throws Exception {
        setBasePath("");
    }

    private void setBasePath(String path) throws Exception {
        PropertiesCache cache = PropertiesCache.getInstance();
        Field field = PropertiesCache.class.getDeclaredField("configProp");
        field.setAccessible(true);
        Properties props = (Properties) field.get(cache);
        props.setProperty(Constants.ACCESS_TOKEN_PUBLICKEY_BASEPATH, path);
    }

    @Test
    void initLoadsValidPublicKeyFromDirectory() throws Exception {
        KeyPairGenerator generator = KeyPairGenerator.getInstance("RSA");
        generator.initialize(2048);
        KeyPair keyPair = generator.generateKeyPair();
        PublicKey expected = keyPair.getPublic();
        String pem = "-----BEGIN PUBLIC KEY-----\n"
                + Base64.getMimeEncoder(64, new byte[]{'\n'}).encodeToString(expected.getEncoded())
                + "\n-----END PUBLIC KEY-----\n";
        Files.writeString(tempDir.resolve("valid-key.pem"), pem, StandardCharsets.UTF_8);
        setBasePath(tempDir.toString());

        keyManager.init();

        KeyData keyData = keyManager.getPublicKey("valid-key.pem");
        assertNotNull(keyData);
        assertArrayEquals(expected.getEncoded(), keyData.getPublicKey().getEncoded());
    }

    @Test
    void initSkipsFileWithInvalidContent() throws Exception {
        Files.writeString(tempDir.resolve("bad-key.pem"), "not-a-valid-key", StandardCharsets.UTF_8);
        setBasePath(tempDir.toString());

        keyManager.init();

        assertNull(keyManager.getPublicKey("bad-key.pem"));
    }

    @Test
    void initLogsErrorWhenBasePathDoesNotExist() throws Exception {
        setBasePath(tempDir.resolve("does-not-exist").toString());

        assertDoesNotThrow(() -> keyManager.init());
    }

    @Test
    void getPublicKeyReturnsNullForUnknownKeyId() {
        assertNull(keyManager.getPublicKey("never-loaded-key-id"));
    }

    @Test
    void loadPublicKeyReturnsKeyForValidPem() throws Exception {
        KeyPairGenerator generator = KeyPairGenerator.getInstance("RSA");
        generator.initialize(2048);
        KeyPair keyPair = generator.generateKeyPair();
        PublicKey expected = keyPair.getPublic();

        String pem = "-----BEGIN PUBLIC KEY-----\n"
                + Base64.getMimeEncoder(64, new byte[]{'\n'}).encodeToString(expected.getEncoded())
                + "\n-----END PUBLIC KEY-----\n";

        PublicKey actual = KeyManager.loadPublicKey(pem);

        assertNotNull(actual);
        assertArrayEquals(expected.getEncoded(), actual.getEncoded());
    }

    @Test
    void loadPublicKeyThrowsPublicKeyLoadExceptionForInvalidKeyBytes() {
        String invalidPem = "-----BEGIN PUBLIC KEY-----\nbm90LWEtdmFsaWQta2V5\n-----END PUBLIC KEY-----\n";

        assertThrows(PublicKeyLoadException.class, () -> KeyManager.loadPublicKey(invalidPem));
    }
}
