package com.igot.cb.authentication.util;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * White-box tests for the package-private Decoder/Encoder state machines.
 * Base64Util.encode/decode always call process() exactly once with finish=true,
 * so the incremental (finish=false, multi-call) code paths below are only
 * reachable by driving Decoder/Encoder directly from within this package.
 */
class Base64UtilCoderTest {

    @Test
    void decoderMaxOutputSizeUsesOverestimateFormula() {
        Base64Util.Decoder decoder = new Base64Util.Decoder(Base64Util.DEFAULT, new byte[0]);

        assertEquals(16, decoder.maxOutputSize(8));
    }

    @Test
    void encoderMaxOutputSizeUsesOverestimateFormula() {
        Base64Util.Encoder encoder = new Base64Util.Encoder(Base64Util.DEFAULT, null);

        assertEquals(18, encoder.maxOutputSize(5));
    }

    @Test
    void decoderProcessImmediatelyReturnsFalseOnceInErrorState() {
        Base64Util.Decoder decoder = new Base64Util.Decoder(Base64Util.DEFAULT, new byte[10]);
        byte[] badInput = "=".getBytes(StandardCharsets.US_ASCII);

        boolean first = decoder.process(badInput, 0, badInput.length, true);
        boolean second = decoder.process(badInput, 0, badInput.length, true);

        assertFalse(first);
        assertFalse(second);
    }

    @Test
    void decoderIncrementalCallsAcrossPartialTupleProduceCorrectOutput() {
        // Base64 for {65,66,67} ("ABC") is "QUJD" - split the input across two calls.
        Base64Util.Decoder decoder = new Base64Util.Decoder(Base64Util.DEFAULT, new byte[10]);
        byte[] part1 = "Q".getBytes(StandardCharsets.US_ASCII);
        byte[] part2 = "UJD".getBytes(StandardCharsets.US_ASCII);

        boolean firstOk = decoder.process(part1, 0, part1.length, false);
        assertTrue(firstOk);
        assertEquals(0, decoder.op);

        boolean secondOk = decoder.process(part2, 0, part2.length, true);
        assertTrue(secondOk);
        assertEquals(3, decoder.op);
        assertEquals(65, decoder.output[0]);
        assertEquals(66, decoder.output[1]);
        assertEquals(67, decoder.output[2]);
    }

    @Test
    void encoderCarriesOneByteTailIntoNextCall() {
        // First call leaves a 1-byte tail; second call combines it with 2 more bytes
        // to emit a full 4-character group ("ABC" -> "QUJD").
        Base64Util.Encoder encoder = new Base64Util.Encoder(Base64Util.NO_WRAP | Base64Util.NO_PADDING, new byte[10]);
        byte[] part1 = new byte[]{65};
        byte[] part2 = new byte[]{66, 67};

        boolean firstOk = encoder.process(part1, 0, part1.length, false);
        assertTrue(firstOk);
        assertEquals(1, encoder.tailLen);
        assertEquals(0, encoder.op);

        boolean secondOk = encoder.process(part2, 0, part2.length, true);
        assertTrue(secondOk);
        assertEquals(0, encoder.tailLen);
        assertEquals(4, encoder.op);
        assertEquals("QUJD", new String(encoder.output, 0, encoder.op, StandardCharsets.US_ASCII));
    }

    @Test
    void encoderCase1TailStaysPendingWhenNotEnoughNewInput() {
        Base64Util.Encoder encoder = new Base64Util.Encoder(Base64Util.NO_WRAP | Base64Util.NO_PADDING, new byte[10]);
        byte[] part1 = new byte[]{65};
        byte[] empty = new byte[0];

        encoder.process(part1, 0, part1.length, false);
        assertEquals(1, encoder.tailLen);

        boolean ok = encoder.process(empty, 0, empty.length, false);

        assertTrue(ok);
        assertEquals(1, encoder.tailLen);
        assertEquals(0, encoder.op);
    }

    @Test
    void encoderCase2TailStaysPendingWhenNotEnoughNewInput() {
        Base64Util.Encoder encoder = new Base64Util.Encoder(Base64Util.NO_WRAP | Base64Util.NO_PADDING, new byte[10]);
        byte[] part1 = new byte[]{65, 66};
        byte[] empty = new byte[0];

        encoder.process(part1, 0, part1.length, false);
        assertEquals(2, encoder.tailLen);

        boolean ok = encoder.process(empty, 0, empty.length, false);

        assertTrue(ok);
        assertEquals(2, encoder.tailLen);
        assertEquals(0, encoder.op);
    }

    @Test
    void encoderFlushesPendingOneByteTailOnFinishWithNoNewInput() {
        // Leaves a 1-byte tail unconsumed by the leading switch(tailLen) block (no new
        // input to combine it with), forcing the finish-block's own tail-aware ternary
        // (tailLen > 0 ? tail[...] : input[...]) to take its true branch.
        Base64Util.Encoder encoder = new Base64Util.Encoder(Base64Util.NO_WRAP | Base64Util.NO_PADDING, new byte[10]);
        byte[] part1 = new byte[]{65};
        byte[] empty = new byte[0];

        encoder.process(part1, 0, part1.length, false);
        assertEquals(1, encoder.tailLen);

        boolean ok = encoder.process(empty, 0, empty.length, true);

        assertTrue(ok);
        assertEquals(0, encoder.tailLen);
        assertEquals(2, encoder.op);
    }

    @Test
    void encoderFlushesPendingTwoByteTailOnFinishWithNoNewInput() {
        Base64Util.Encoder encoder = new Base64Util.Encoder(Base64Util.NO_WRAP | Base64Util.NO_PADDING, new byte[10]);
        byte[] part1 = new byte[]{65, 66};
        byte[] empty = new byte[0];

        encoder.process(part1, 0, part1.length, false);
        assertEquals(2, encoder.tailLen);

        boolean ok = encoder.process(empty, 0, empty.length, true);

        assertTrue(ok);
        assertEquals(0, encoder.tailLen);
        assertEquals(3, encoder.op);
    }

    @Test
    void encoderCarriesTwoByteTailIntoNextCall() {
        // First call leaves a 2-byte tail; second call combines it with 1 more byte
        // to emit a full 4-character group ("ABC" -> "QUJD").
        Base64Util.Encoder encoder = new Base64Util.Encoder(Base64Util.NO_WRAP | Base64Util.NO_PADDING, new byte[10]);
        byte[] part1 = new byte[]{65, 66};
        byte[] part2 = new byte[]{67};

        boolean firstOk = encoder.process(part1, 0, part1.length, false);
        assertTrue(firstOk);
        assertEquals(2, encoder.tailLen);
        assertEquals(0, encoder.op);

        boolean secondOk = encoder.process(part2, 0, part2.length, true);
        assertTrue(secondOk);
        assertEquals(0, encoder.tailLen);
        assertEquals(4, encoder.op);
        assertEquals("QUJD", new String(encoder.output, 0, encoder.op, StandardCharsets.US_ASCII));
    }
}
