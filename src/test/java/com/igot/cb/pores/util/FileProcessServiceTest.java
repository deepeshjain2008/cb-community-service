package com.igot.cb.pores.util;

import com.igot.cb.pores.exceptions.CustomException;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class FileProcessServiceTest {

    private final FileProcessService fileProcessService = new FileProcessService();

    private InputStream asStream(String content) {
        return new ByteArrayInputStream(content.getBytes(StandardCharsets.UTF_8));
    }

    @Test
    void processCsvAndSendMessageParsesRowsUntilBlankRow() throws IOException {
        String csv = "name,email\nAlice,alice@example.com\nBob,bob@example.com\n";

        List<Map<String, String>> rows = fileProcessService.processCsvAndSendMessage(asStream(csv));

        assertEquals(2, rows.size());
        assertEquals("Alice", rows.get(0).get("name"));
        assertEquals("bob@example.com", rows.get(1).get("email"));
    }

    @Test
    void processCsvAndSendMessageStopsAtFullyBlankRow() throws IOException {
        String csv = "name,email\nAlice,alice@example.com\n,\nBob,bob@example.com\n";

        List<Map<String, String>> rows = fileProcessService.processCsvAndSendMessage(asStream(csv));

        assertEquals(1, rows.size());
        assertEquals("Alice", rows.get(0).get("name"));
    }

    @Test
    void processCsvAndSendMessageTrimsAndReplacesNewlinesInCellValue() throws IOException {
        String csv = "name,notes\nAlice,\"line1\nline2\"\n";

        List<Map<String, String>> rows = fileProcessService.processCsvAndSendMessage(asStream(csv));

        assertEquals(1, rows.size());
        assertEquals("line1,line2", rows.get(0).get("notes"));
    }

    @Test
    void processCsvAndSendMessageReturnsEmptyListForHeaderOnlyFile() throws IOException {
        String csv = "name,email\n";

        List<Map<String, String>> rows = fileProcessService.processCsvAndSendMessage(asStream(csv));

        assertTrue(rows.isEmpty());
    }

    @Test
    void processCsvAndSendMessageThrowsCustomExceptionOnMalformedCsv() {
        String malformed = "name,email\n\"unclosed quote,value\n";

        assertThrows(CustomException.class,
            () -> fileProcessService.processCsvAndSendMessage(asStream(malformed)));
    }
}
