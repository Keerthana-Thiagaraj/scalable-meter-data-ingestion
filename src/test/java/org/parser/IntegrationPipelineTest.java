package org.parser;

import org.junit.jupiter.api.*;
import java.io.FileWriter;
import java.sql.*;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * IntegrationPipelineTest verifies the end-to-end pipeline:
 * parsing NEM12 files, inserting readings into the database, and error/audit logging.
 */
class IntegrationPipelineTest {
    private static Connection conn;
    private MeterReadingSqlGenerator sqlGen;
    private NEM12Parser parser;
    private List<MeterReading> readings;
    private List<String> auditLogs;
    private static final String TEST_FILE = "integration_nem12.csv";

    /**
     * Sets up the in-memory H2 database before all tests.
     */
    @BeforeAll
    static void setupDB() throws Exception {
        conn = DriverManager.getConnection("jdbc:h2:mem:testdb2;MODE=PostgreSQL", "sa", "");
        try (Statement st = conn.createStatement()) {
            // Removed CREATE EXTENSION statement; not needed for H2
            st.execute("CREATE TABLE meter_readings (id uuid DEFAULT random_uuid() NOT NULL, nmi varchar(10) NOT NULL, timestamp timestamp NOT NULL, consumption numeric NOT NULL, CONSTRAINT meter_readings_pk PRIMARY KEY (id), CONSTRAINT meter_readings_unique_consumption UNIQUE (nmi, timestamp));");
        }
    }

    /**
     * Tears down the database after all tests.
     */
    @AfterAll
    static void tearDownDB() throws Exception {
        conn.close();
    }

    /**
     * Initializes objects before each test.
     */
    @BeforeEach
    void setUp() {
        sqlGen = new MeterReadingSqlGenerator("jdbc:h2:mem:testdb2;MODE=PostgreSQL", "sa", "", 2);
        parser = new NEM12Parser(new ErrorLogger("integration_error_log.csv"), 2);
        readings = new ArrayList<>();
        auditLogs = new ArrayList<>();
    }

    /**
     * Utility to write a test NEM12 file.
     */
    void writeTestFile(String content) throws Exception {
        try (FileWriter fw = new FileWriter(TEST_FILE)) {
            fw.write(content);
        }
    }

    /**
     * Tests the full pipeline: parsing, inserting, and logging.
     */
    @Test
    void testFullPipeline() throws Exception {
        // Prepare a valid NEM12 file with two days of readings for one NMI
        String content = """
            100,HEADER
            200,NMI10,2
            300,2025-09-12,1.1,2.2
            300,2025-09-13,3.3,4.4
            900,FOOTER
            """;
        writeTestFile(content);
        long start = System.currentTimeMillis();
        // Parse the file and collect readings
        parser.parse(TEST_FILE, readings::add, auditLogs::add);
        long parseEnd = System.currentTimeMillis();
        // Insert readings into the DB and collect audit logs
        sqlGen.insertReadings(readings, auditLogs::add); // removed unused 'inserted' variable
        long insertEnd = System.currentTimeMillis();
        // Calculate and log parse/insert rates
        double parseRate = readings.size() / ((parseEnd - start) / 1000.0);
        double insertRate = readings.size() / ((insertEnd - parseEnd) / 1000.0);
        auditLogs.add(String.format("Parse rate: %.2f rows/sec, Insert rate: %.2f rows/sec", parseRate, insertRate));
        // Verify DB state: should have 4 rows (2 days x 2 intervals)
        try (Statement st = conn.createStatement(); ResultSet rs = st.executeQuery("SELECT COUNT(*) FROM meter_readings")) {
            rs.next();
            assertEquals(4, rs.getInt(1));
        }
        // Check audit logs for metrics
        assertTrue(auditLogs.stream().anyMatch(log -> log.contains("Parse rate")), "Audit log should contain parse rate");
        assertTrue(auditLogs.stream().anyMatch(log -> log.contains("Insert rate")), "Audit log should contain insert rate");
    }
}
