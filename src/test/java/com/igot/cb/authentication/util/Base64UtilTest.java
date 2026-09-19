package com.igot.cb.authentication.util;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class Base64UtilTest {

    @Test
    void encodeDecodeRoundTripWithDefaultFlags() {
        byte[] data = "hello world".getBytes(StandardCharsets.UTF_8);

        String encoded = Base64Util.encodeToString(data, Base64Util.DEFAULT);
        byte[] decoded = Base64Util.decode(encoded, Base64Util.DEFAULT);

        assertArrayEquals(data, decoded);
    }

    @Test
    void encodeDecodeRoundTripWithNoPadding() {
        byte[] data = "ab".getBytes(StandardCharsets.UTF_8);

        String encoded = Base64Util.encodeToString(data, Base64Util.NO_PADDING | Base64Util.NO_WRAP);
        assertEquals(false, encoded.contains("="));
        byte[] decoded = Base64Util.decode(encoded, Base64Util.NO_PADDING | Base64Util.NO_WRAP);

        assertArrayEquals(data, decoded);
    }

    @Test
    void encodeDecodeRoundTripWithUrlSafeAlphabet() {
        byte[] data = new byte[]{(byte) 0xFB, (byte) 0xFF, (byte) 0xBE};

        String encoded = Base64Util.encodeToString(data, Base64Util.URL_SAFE | Base64Util.NO_WRAP);
        byte[] decoded = Base64Util.decode(encoded, Base64Util.URL_SAFE);

        assertArrayEquals(data, decoded);
    }

    @Test
    void encodeWithCrlfWrapsLongInputWithCarriageReturns() {
        byte[] data = new byte[120];
        Arrays.fill(data, (byte) 'A');

        String encoded = Base64Util.encodeToString(data, Base64Util.CRLF);

        assertEquals(true, encoded.contains("\r\n"));
    }

    @Test
    void encodeWithDefaultWrapsLongInputWithNewlineOnly() {
        byte[] data = new byte[200];
        Arrays.fill(data, (byte) 'Z');

        String encoded = Base64Util.encodeToString(data, Base64Util.DEFAULT);

        assertEquals(true, encoded.contains("\n"));
        assertEquals(false, encoded.contains("\r\n"));
        byte[] decoded = Base64Util.decode(encoded, Base64Util.DEFAULT);
        assertArrayEquals(data, decoded);
    }

    @Test
    void encodeWithNoWrapProducesSingleLine() {
        byte[] data = new byte[200];
        Arrays.fill(data, (byte) 'Z');

        String encoded = Base64Util.encodeToString(data, Base64Util.NO_WRAP);

        assertEquals(false, encoded.contains("\n"));
    }

    @Test
    void decodeToleratesEmbeddedWhitespace() {
        byte[] data = "hello world".getBytes(StandardCharsets.UTF_8);
        String encoded = Base64Util.encodeToString(data, Base64Util.NO_WRAP);
        String withWhitespace = encoded.substring(0, 4) + "\n " + encoded.substring(4);

        byte[] decoded = Base64Util.decode(withWhitespace, Base64Util.DEFAULT);

        assertArrayEquals(data, decoded);
    }

    @Test
    void decodeHandlesSinglePaddingCharacter() {
        byte[] data = "abcdefgh".getBytes(StandardCharsets.UTF_8);
        String encoded = Base64Util.encodeToString(data, Base64Util.NO_WRAP);

        byte[] decoded = Base64Util.decode(encoded, Base64Util.DEFAULT);

        assertArrayEquals(data, decoded);
    }

    @Test
    void decodeHandlesDoublePaddingCharacter() {
        byte[] data = "abcdefg".getBytes(StandardCharsets.UTF_8);
        String encoded = Base64Util.encodeToString(data, Base64Util.NO_WRAP);

        byte[] decoded = Base64Util.decode(encoded, Base64Util.DEFAULT);

        assertArrayEquals(data, decoded);
    }

    @Test
    void decodeThrowsForPaddingCharacterAtStart() {
        assertThrows(IllegalArgumentException.class, () -> Base64Util.decode("=abc", Base64Util.DEFAULT));
    }

    @Test
    void decodeThrowsForExcessivePadding() {
        assertThrows(IllegalArgumentException.class, () -> Base64Util.decode("YQ====", Base64Util.DEFAULT));
    }

    @Test
    void decodeThrowsForDataLengthOneModFour() {
        assertThrows(IllegalArgumentException.class, () -> Base64Util.decode("Y", Base64Util.DEFAULT));
    }

    @Test
    void decodeHandlesEmptyInput() {
        byte[] decoded = Base64Util.decode("", Base64Util.DEFAULT);

        assertEquals(0, decoded.length);
    }

    @Test
    void encodeHandlesEmptyInput() {
        String encoded = Base64Util.encodeToString(new byte[0], Base64Util.DEFAULT);

        assertEquals("", encoded);
    }

    @Test
    void decodeWithOffsetAndLengthDecodesSubrange() {
        byte[] data = "hello world".getBytes(StandardCharsets.UTF_8);
        String encoded = Base64Util.encodeToString(data, Base64Util.NO_WRAP);
        byte[] prefixed = ("XX" + encoded).getBytes(StandardCharsets.UTF_8);

        byte[] decoded = Base64Util.decode(prefixed, 2, encoded.length(), Base64Util.DEFAULT);

        assertArrayEquals(data, decoded);
    }

    @Test
    void encodeWithOffsetAndLengthEncodesSubrange() {
        byte[] data = "XXhello world".getBytes(StandardCharsets.UTF_8);
        byte[] expected = "hello world".getBytes(StandardCharsets.UTF_8);

        String encoded = Base64Util.encodeToString(data, 2, data.length - 2, Base64Util.NO_WRAP);
        byte[] decoded = Base64Util.decode(encoded, Base64Util.DEFAULT);

        assertArrayEquals(expected, decoded);
    }

    @Test
    void encodeOneByteRemainderUsesTwoOutputCharacters() {
        byte[] data = new byte[]{65};

        String encoded = Base64Util.encodeToString(data, Base64Util.NO_PADDING | Base64Util.NO_WRAP);

        assertEquals(2, encoded.length());
        byte[] decoded = Base64Util.decode(encoded, Base64Util.NO_PADDING | Base64Util.NO_WRAP);
        assertArrayEquals(data, decoded);
    }

    @Test
    void encodeTwoByteRemainderUsesThreeOutputCharacters() {
        byte[] data = new byte[]{65, 66};

        String encoded = Base64Util.encodeToString(data, Base64Util.NO_PADDING | Base64Util.NO_WRAP);

        assertEquals(3, encoded.length());
        byte[] decoded = Base64Util.decode(encoded, Base64Util.NO_PADDING | Base64Util.NO_WRAP);
        assertArrayEquals(data, decoded);
    }

    @Test
    void decodeThrowsWhenSinglePaddingCharacterIsTruncated() {
        // "QQ==" is the valid single-padding encoding of a 1-byte input; dropping the
        // trailing '=' leaves the decoder mid-way through state 4 when input runs out.
        assertThrows(IllegalArgumentException.class, () -> Base64Util.decode("QQ=", Base64Util.DEFAULT));
    }

    @Test
    void decodeThrowsForPaddingCharacterAtSecondPosition() {
        // A '=' appearing as only the second character (decoder state 1) has no valid
        // handling - only states 2/3/4 recognize EQUALS specially.
        assertThrows(IllegalArgumentException.class, () -> Base64Util.decode("Q=", Base64Util.DEFAULT));
    }

    @Test
    void decodeThrowsWhenDataCharacterFollowsSinglePadding() {
        // After the single padding character in "QQ=" is consumed (state 4), a regular
        // data character instead of the expected second '=' is an error.
        assertThrows(IllegalArgumentException.class, () -> Base64Util.decode("QQ=A", Base64Util.DEFAULT));
    }

    @Test
    void encodeWithExactMultipleOfThreeStillAppendsTrailingNewline() {
        byte[] data = new byte[6];
        Arrays.fill(data, (byte) 'A');

        String encoded = Base64Util.encodeToString(data, Base64Util.DEFAULT);

        assertEquals(true, encoded.endsWith("\n"));
        byte[] decoded = Base64Util.decode(encoded, Base64Util.DEFAULT);
        assertArrayEquals(data, decoded);
    }
}
