package com.igot.cb.authentication.util;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.igot.cb.authentication.model.KeyData;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.keycloak.common.util.Time;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.Signature;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class AccessTokenValidatorTest {

    private static final String REALM_URL = "https://portal.dev.karmayogibharat.net/auth/realms/sunbird";
    private static final String KEY_ID = "test-key-id";
    private static final ObjectMapper mapper = new ObjectMapper();

    @Mock
    private KeyManager keyManager;

    @InjectMocks
    private AccessTokenValidator accessTokenValidator;

    private PrivateKey privateKey;
    private PublicKey publicKey;

    @BeforeEach
    void setUp() throws Exception {
        KeyPairGenerator generator = KeyPairGenerator.getInstance("RSA");
        generator.initialize(2048);
        KeyPair keyPair = generator.generateKeyPair();
        privateKey = keyPair.getPrivate();
        publicKey = keyPair.getPublic();
    }

    @Test
    void verifyUserTokenReturnsUserIdForValidToken() throws Exception {
        lenient().when(keyManager.getPublicKey(KEY_ID)).thenReturn(new KeyData(KEY_ID, publicKey));
        String token = buildToken(REALM_URL, "f:123:user1", Time.currentTime() + 3600);

        String userId = accessTokenValidator.verifyUserToken(token);

        assertEquals("user1", userId);
    }

    @Test
    void verifyUserTokenReturnsNullForExpiredToken() throws Exception {
        lenient().when(keyManager.getPublicKey(KEY_ID)).thenReturn(new KeyData(KEY_ID, publicKey));
        String token = buildToken(REALM_URL, "f:123:user1", Time.currentTime() - 3600);

        String userId = accessTokenValidator.verifyUserToken(token);

        assertNull(userId);
    }

    @Test
    void verifyUserTokenReturnsNullForInvalidIssuer() throws Exception {
        lenient().when(keyManager.getPublicKey(KEY_ID)).thenReturn(new KeyData(KEY_ID, publicKey));
        String token = buildToken("https://untrusted.example.com/auth/realms/other", "f:123:user1", Time.currentTime() + 3600);

        String userId = accessTokenValidator.verifyUserToken(token);

        assertNull(userId);
    }

    @Test
    void verifyUserTokenReturnsNullForMalformedToken() {
        String userId = accessTokenValidator.verifyUserToken("not-a-valid-token");

        assertNull(userId);
    }

    @Test
    void fetchUserIdFromAccessTokenReturnsNullForNullToken() {
        assertNull(accessTokenValidator.fetchUserIdFromAccessToken(null));
    }

    @Test
    void fetchUserIdFromAccessTokenReturnsUserIdForValidToken() throws Exception {
        lenient().when(keyManager.getPublicKey(KEY_ID)).thenReturn(new KeyData(KEY_ID, publicKey));
        String token = buildToken(REALM_URL, "f:123:user1", Time.currentTime() + 3600);

        String userId = accessTokenValidator.fetchUserIdFromAccessToken(token);

        assertEquals("user1", userId);
    }

    @Test
    void verifyUserTokenReturnsNullWhenSignatureInvalid() throws Exception {
        KeyPairGenerator generator = KeyPairGenerator.getInstance("RSA");
        generator.initialize(2048);
        PublicKey otherPublicKey = generator.generateKeyPair().getPublic();
        lenient().when(keyManager.getPublicKey(KEY_ID)).thenReturn(new KeyData(KEY_ID, otherPublicKey));
        String token = buildToken(REALM_URL, "f:123:user1", Time.currentTime() + 3600);

        String userId = accessTokenValidator.verifyUserToken(token);

        assertNull(userId);
    }

    @Test
    void verifyUserTokenReturnsBlankSubjectWhenSubjectIsBlank() throws Exception {
        lenient().when(keyManager.getPublicKey(KEY_ID)).thenReturn(new KeyData(KEY_ID, publicKey));
        String token = buildToken(REALM_URL, "", Time.currentTime() + 3600);

        String userId = accessTokenValidator.verifyUserToken(token);

        assertEquals("", userId);
    }

    @Test
    void fetchUserIdFromAccessTokenReturnsNullWhenSubjectNotAString() throws Exception {
        lenient().when(keyManager.getPublicKey(KEY_ID)).thenReturn(new KeyData(KEY_ID, publicKey));
        String token = buildTokenWithNonStringSubject(REALM_URL, 12345, Time.currentTime() + 3600);

        String userId = accessTokenValidator.fetchUserIdFromAccessToken(token);

        assertNull(userId);
    }

    @Test
    void verifyUserTokenReturnsNullWhenHeaderMissingKid() throws Exception {
        String token = buildTokenWithoutKid(REALM_URL, "f:123:user1", Time.currentTime() + 3600);

        String userId = accessTokenValidator.verifyUserToken(token);

        assertNull(userId);
    }

    private String buildToken(String issuer, String subject, int expiry) throws Exception {
        return buildTokenWithNonStringSubject(issuer, subject, expiry);
    }

    private String buildTokenWithNonStringSubject(String issuer, Object subject, int expiry) throws Exception {
        Map<String, Object> header = new HashMap<>();
        header.put("alg", "RS256");
        header.put("kid", KEY_ID);

        Map<String, Object> body = new HashMap<>();
        body.put("sub", subject);
        body.put("iss", issuer);
        body.put("exp", expiry);

        return signPayload(header, body);
    }

    private String buildTokenWithoutKid(String issuer, String subject, int expiry) throws Exception {
        Map<String, Object> header = new HashMap<>();
        header.put("alg", "RS256");

        Map<String, Object> body = new HashMap<>();
        body.put("sub", subject);
        body.put("iss", issuer);
        body.put("exp", expiry);

        return signPayload(header, body);
    }

    private String signPayload(Map<String, Object> header, Map<String, Object> body) throws Exception {
        String encodedHeader = base64UrlEncode(mapper.writeValueAsBytes(header));
        String encodedBody = base64UrlEncode(mapper.writeValueAsBytes(body));
        String payload = encodedHeader + "." + encodedBody;

        Signature signature = Signature.getInstance("SHA256withRSA");
        signature.initSign(privateKey);
        signature.update(payload.getBytes(StandardCharsets.US_ASCII));
        String encodedSignature = base64UrlEncode(signature.sign());

        return payload + "." + encodedSignature;
    }

    private String base64UrlEncode(byte[] data) {
        return Base64Util.encodeToString(data, Base64Util.URL_SAFE | Base64Util.NO_PADDING | Base64Util.NO_WRAP);
    }
}
