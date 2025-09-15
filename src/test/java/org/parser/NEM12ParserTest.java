package org.parser;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * NEM12ParserTest verifies parsing logic, error handling, and audit logging for NEM12Parser.
 */
class NEM12ParserTest {
    private ErrorLogger errorLogger;
    private List<MeterReading> readings;
    private List<String> auditLogs;
    private NEM12Parser parser;
    private static final String TEST_FILE = "test_nem12.csv";

    /**
     * Initializes objects before each test.
     */
    @BeforeEach
    void setUp() {
        errorLogger = new ErrorLogger("test_error_log.csv");
        readings = new ArrayList<>();
        auditLogs = new ArrayList<>();
        parser = new NEM12Parser(errorLogger, 3); // 3 intervals for test
    }

    /**
     * Utility to write a test NEM12 file.
     */
    void writeTestFile(String content) throws IOException {
        try (FileWriter fw = new FileWriter(TEST_FILE)) {
            fw.write(content);
        }
    }

    /**
     * Tests parsing of a valid NEM12 file and audit logging.
     */
    @Test
    void testValidFile() throws IOException {
        String content = """
            100,HEADER
            200,NMI123,3
            300,2025-09-12,1.1,2.2,3.3
            900,FOOTER
            """;
        writeTestFile(content);
        parser.parse(TEST_FILE, readings::add, auditLogs::add);
        assertEquals(3, readings.size());
        assertTrue(auditLogs.getFirst().contains("Rows inserted: 3"));
    }

    @Test
    void testMissing100Or900() throws IOException {
        String content = """
            200,NMI123,3
            300,2025-09-12,1.1,2.2,3.3
            """;
        writeTestFile(content);
        assertThrows(IOException.class, () -> parser.parse(TEST_FILE, readings::add, auditLogs::add));
    }

    @Test
    void testIntervalCountMismatch() throws IOException {
        String content = """
            100,HEADER
            200,NMI123,3
            300,2025-09-12,1.1,2.2
            900,FOOTER
            """;
        writeTestFile(content);
        parser.parse(TEST_FILE, readings::add, auditLogs::add);
        assertEquals(0, readings.size());
        assertTrue(auditLogs.getFirst().contains("Rows inserted: 0"));
    }

    @Test
    void testNonNumericConsumption() throws IOException {
        String content = """
            100,HEADER
            200,NMI123,3
            300,2025-09-12,1.1,abc,3.3
            900,FOOTER
            """;
        writeTestFile(content);
        parser.parse(TEST_FILE, readings::add, auditLogs::add);
        assertEquals(2, readings.size());
        assertTrue(auditLogs.getFirst().contains("Rows inserted: 2"));
    }

    @Test
    void testMalformedRecord() throws IOException {
        String content = """
            100,HEADER
            200,NMI123
            300,2025-09-12,1.1,2.2,3.3
            900,FOOTER
            """;
        writeTestFile(content);
        parser.parse(TEST_FILE, readings::add, auditLogs::add);
        assertEquals(0, readings.size());
        assertTrue(auditLogs.getFirst().contains("Rows inserted: 0"));
    }

    @Test
    void testMultipleNMIs() throws IOException {
        String content = """
            100,HEADER
            200,NMI123,3
            300,2025-09-12,1.1,2.2,3.3
            200,NMI456,3
            300,2025-09-13,4.4,5.5,6.6
            900,FOOTER
            """;
        writeTestFile(content);
        parser.parse(TEST_FILE, readings::add, auditLogs::add);
        assertEquals(6, readings.size());
        assertTrue(auditLogs.getFirst().contains("Rows inserted: 6"));
    }

    @Test
    void test500RecordIgnored() throws IOException {
        String content = """
            100,HEADER
            200,NMI123,3
            300,2025-09-12,1.1,2.2,3.3
            500,IGNORED
            900,FOOTER
            """;
        writeTestFile(content);
        parser.parse(TEST_FILE, readings::add, auditLogs::add);
        assertEquals(3, readings.size());
        assertTrue(auditLogs.getFirst().contains("Rows inserted: 3"));
    }

    @Test
    void testExtraFieldsIn300Record() throws IOException {
        String content = """
            100,HEADER
            200,NMI123,3
            300,2025-09-12,1.1,2.2,3.3,7.7
            900,FOOTER
            """;
        writeTestFile(content);
        parser.parse(TEST_FILE, readings::add, auditLogs::add);
        assertEquals(0, readings.size());
        assertTrue(auditLogs.getFirst().contains("Rows inserted: 0"));
    }

    @Test
    void testEmptyOrWhitespaceLines() throws IOException {
        String content = """
            100,HEADER
            
            200,NMI123,3
               
            300,2025-09-12,1.1,2.2,3.3
            900,FOOTER
            """;
        writeTestFile(content);
        parser.parse(TEST_FILE, readings::add, auditLogs::add);
        assertEquals(3, readings.size());
        assertTrue(auditLogs.getFirst().contains("Rows inserted: 3"));
    }

    @Test
    void testAllIntervalsNonNumeric() throws IOException {
        String content = """
            100,HEADER
            200,NMI123,3
            300,2025-09-12,abc,def,ghi
            900,FOOTER
            """;
        writeTestFile(content);
        parser.parse(TEST_FILE, readings::add, auditLogs::add);
        assertEquals(0, readings.size());
        assertTrue(auditLogs.getFirst().contains("Rows inserted: 0"));
    }

    @Test
    void testOnlyHeaderAndFooter() throws IOException {
        String content = """
            100,HEADER
            900,FOOTER
            """;
        writeTestFile(content);
        parser.parse(TEST_FILE, readings::add, auditLogs::add);
        assertEquals(0, readings.size());
        assertTrue(auditLogs.getFirst().contains("Rows inserted: 0"));
    }

    @Test
    void testRedundantFileUpload() throws IOException {
        String content = """
            100,HEADER
            200,NMI123,3
            300,2025-09-12,1.1,2.2,3.3
            900,FOOTER
            """;
        writeTestFile(content);
        parser.parse(TEST_FILE, readings::add, auditLogs::add);
        int firstParseCount = readings.size();
        parser.parse(TEST_FILE, readings::add, auditLogs::add);
        int secondParseCount = readings.size();
        assertEquals(firstParseCount * 2, secondParseCount);
        assertTrue(auditLogs.get(1).contains("Rows inserted: 3"));
    }

    @Test
    void testErrorFileContent() throws IOException {
        String content = """
            100,HEADER
            200,NMI123,3
            300,2025-09-12,abc,def,ghi
            900,FOOTER
            """;
        writeTestFile(content);
        parser.parse(TEST_FILE, readings::add, auditLogs::add);
        try (java.io.BufferedReader br = new java.io.BufferedReader(new java.io.FileReader("test_error_log.csv"))) {
            String line;
            boolean found = false;
            while ((line = br.readLine()) != null) {
                if (line.contains("Non-numeric consumption value")) {
                    found = true;
                    break;
                }
            }
            assertTrue(found, "Error log should contain non-numeric value error");
        }
    }

    @Test
    void testAuditLogContent() throws IOException {
        String content = """
            100,HEADER
            200,NMI123,3
            300,2025-09-12,1.1,2.2,3.3
            900,FOOTER
            """;
        writeTestFile(content);
        parser.parse(TEST_FILE, readings::add, auditLogs::add);
        String audit = auditLogs.getFirst();
        assertTrue(audit.contains("Rows inserted: 3"));
        assertTrue(audit.contains("Errors: 0") || audit.contains("Errors: 1") || audit.contains("Errors: 2"));
    }

    @Test
    void testBoundaryValues() throws IOException {
        String content = """
            100,HEADER
            200,NMI123,3
            300,2025-09-12,0,9999999999,-9999999999
            900,FOOTER
            """;
        writeTestFile(content);
        parser.parse(TEST_FILE, readings::add, auditLogs::add);
        assertEquals(3, readings.size());
        assertTrue(auditLogs.getFirst().contains("Rows inserted: 3"));
    }

    @Test
    void testUnicodeNMI() throws IOException {
        String content = """
            100,HEADER
            200,测试NMI,3
            300,2025-09-12,1.1,2.2,3.3
            900,FOOTER
            """;
        writeTestFile(content);
        parser.parse(TEST_FILE, readings::add, auditLogs::add);
        assertEquals(3, readings.size());
        assertTrue(auditLogs.getFirst().contains("Rows inserted: 3"));
    }
}
