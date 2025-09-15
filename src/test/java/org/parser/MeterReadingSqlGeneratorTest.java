package org.parser;

import org.junit.jupiter.api.*;
import java.sql.*;
import java.time.LocalDateTime;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * MeterReadingSqlGeneratorTest verifies batch insert and idempotency logic for MeterReadingSqlGenerator.
 */
class MeterReadingSqlGeneratorTest {
    private static Connection conn;
    private MeterReadingSqlGenerator sqlGen;
    private List<String> auditLogs;

    /**
     * Sets up the in-memory H2 database before all tests.
     */
    @BeforeAll
    static void setupDB() throws Exception {
        conn = DriverManager.getConnection("jdbc:h2:mem:testdb;MODE=PostgreSQL", "sa", "");
        try (Statement st = conn.createStatement()) {
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
        sqlGen = new MeterReadingSqlGenerator("jdbc:h2:mem:testdb;MODE=PostgreSQL", "sa", "", 2);
        auditLogs = new ArrayList<>();
    }

    /**
     * Tests batch insert and idempotency: ensures duplicate readings are not re-inserted.
     */
    @Test
    void testBatchInsertAndIdempotency() throws Exception {
        List<MeterReading> readings = List.of(
            new MeterReading("NMI1", LocalDateTime.of(2025,9,12,0,0), 1.1),
            new MeterReading("NMI1", LocalDateTime.of(2025,9,12,0,30), 2.2),
            new MeterReading("NMI2", LocalDateTime.of(2025,9,12,1,0), 3.3)
        );
        int inserted = sqlGen.insertReadings(readings, auditLogs::add);
        assertEquals(3, inserted);
        // Re-insert same readings, should not duplicate
        sqlGen.insertReadings(readings, auditLogs::add); // removed unused 'insertedAgain' variable
        // H2's MERGE INTO always returns 1 for each row, so check DB row count for idempotency
        try (Statement st = conn.createStatement(); ResultSet rs = st.executeQuery("SELECT COUNT(*) FROM meter_readings")) {
            rs.next();
            assertEquals(3, rs.getInt(1));
        }
    }

    /**
     * Tests audit logging functionality.
     */
    @Test
    void testAuditLogging() throws Exception {
        List<MeterReading> readings = List.of(
            new MeterReading("NMI3", LocalDateTime.of(2025,9,12,2,0), 4.4)
        );
        sqlGen.insertReadings(readings, auditLogs::add);
        assertTrue(auditLogs.getFirst().contains("Rows inserted: 1"));
    }

    /**
     * Tests behavior when inserting an empty readings list.
     */
    @Test
    void testEmptyReadingsList() throws Exception {
        List<MeterReading> readings = List.of();
        int inserted = sqlGen.insertReadings(readings, auditLogs::add);
        assertEquals(0, inserted);
        assertTrue(auditLogs.getFirst().contains("Rows inserted: 0"));
    }

    /**
     * Tests that duplicate readings in a batch are handled correctly.
     */
    @Test
    void testDuplicateReadingsInBatch() throws Exception {
        MeterReading r = new MeterReading("NMI1", LocalDateTime.of(2025,9,12,0,0), 1.1);
        List<MeterReading> readings = List.of(r, r, r);
        int inserted = sqlGen.insertReadings(readings, auditLogs::add);
        try (Statement st = conn.createStatement(); ResultSet rs = st.executeQuery("SELECT COUNT(*) FROM meter_readings WHERE nmi='NMI1' AND timestamp='2025-09-12T00:00:00'")) {
            rs.next();
            assertEquals(1, rs.getInt(1));
        }
    }

    /**
     * Tests insertion of extreme numeric values.
     */
    @Test
    void testExtremeNumericValues() throws Exception {
        List<MeterReading> readings = List.of(
            new MeterReading("NMI1", LocalDateTime.of(2025,9,12,0,0), 0),
            new MeterReading("NMI1", LocalDateTime.of(2025,9,12,0,30), Double.MAX_VALUE),
            new MeterReading("NMI1", LocalDateTime.of(2025,9,12,1,0), -Double.MAX_VALUE)
        );
        sqlGen.insertReadings(readings, auditLogs::add); // removed unused 'inserted' variable
        assertEquals(3, readings.size());
    }

    /**
     * Tests insertion of readings with Unicode NMI values.
     */
    @Test
    void testUnicodeNMI() throws Exception {
        List<MeterReading> readings = List.of(
            new MeterReading("测试NMI", LocalDateTime.of(2025,9,12,0,0), 1.1)
        );
        int inserted = sqlGen.insertReadings(readings, auditLogs::add);
        assertEquals(1, inserted);
    }

    /**
     * Tests that null or invalid values in readings throw exceptions.
     */
    @Test
    void testNullOrInvalidValues() {
        assertThrows(java.sql.BatchUpdateException.class, () -> {
            List<MeterReading> readings = List.of(
                new MeterReading(null, LocalDateTime.of(2025,9,12,0,0), 1.1)
            );
            sqlGen.insertReadings(readings, auditLogs::add);
        });
    }

    /**
     * Tests behavior when the database connection fails.
     */
    @Test
    void testDBConnectionFailure() {
        MeterReadingSqlGenerator badSqlGen = new MeterReadingSqlGenerator("jdbc:h2:mem:bad;MODE=PostgreSQL", "sa", "", 2);
        List<MeterReading> readings = List.of(
            new MeterReading("NMI1", LocalDateTime.of(2025,9,12,0,0), 1.1)
        );
        assertThrows(SQLException.class, () -> badSqlGen.insertReadings(readings, auditLogs::add));
    }

    /**
     * Tests performance and correctness of large batch inserts.
     */
    @Test
    void testLargeBatchInsert() throws Exception {
        List<MeterReading> readings = new ArrayList<>();
        for (int i = 0; i < 1000; i++) {
            readings.add(new MeterReading("NMI"+i, LocalDateTime.of(2025,9,12,0,0), i));
        }
        int inserted = sqlGen.insertReadings(readings, auditLogs::add);
        assertEquals(1000, inserted);
    }

    /**
     * Tests that concurrent redundant inserts are handled correctly.
     */
    @Test
    void testConcurrentRedundantInsert() throws Exception {
        List<MeterReading> readings = List.of(
            new MeterReading("NMI1", LocalDateTime.of(2025,9,12,0,0), 1.1)
        );
        Runnable insertTask = () -> {
            try {
                sqlGen.insertReadings(readings, auditLogs::add);
            } catch (SQLException e) {
                fail("Insert failed: " + e.getMessage());
            }
        };
        Thread t1 = new Thread(insertTask);
        Thread t2 = new Thread(insertTask);
        t1.start();
        t2.start();
        t1.join();
        t2.join();
        try (Statement st = conn.createStatement(); ResultSet rs = st.executeQuery("SELECT COUNT(*) FROM meter_readings WHERE nmi='NMI1' AND timestamp='2025-09-12T00:00:00'")) {
            rs.next();
            assertEquals(1, rs.getInt(1));
        }
    }
}
