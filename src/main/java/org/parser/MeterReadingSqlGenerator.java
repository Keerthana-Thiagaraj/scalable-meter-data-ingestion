package org.parser;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import java.util.function.Consumer;

/**
 * MeterReadingSqlGenerator handles batch inserts of MeterReading objects into the database.
 * Uses prepared statements for security and idempotency, and supports both PostgreSQL and H2.
 */
public class MeterReadingSqlGenerator {
    private final String dbUrl;
    private final String dbUser;
    private final String dbPassword;
    private final int batchSize;

    /**
     * @param dbUrl JDBC URL for the database
     * @param dbUser Database username
     * @param dbPassword Database password
     * @param batchSize Number of rows per batch insert
     */
    public MeterReadingSqlGenerator(String dbUrl, String dbUser, String dbPassword, int batchSize) {
        this.dbUrl = dbUrl;
        this.dbUser = dbUser;
        this.dbPassword = dbPassword;
        this.batchSize = batchSize;
    }

    /**
     * Executes a batch and counts the number of rows inserted (returns 1 for each inserted row).
     */
    private int executeBatchAndCount(PreparedStatement ps) throws SQLException {
        int inserted = 0;
        for (int r : ps.executeBatch()) if (r == 1) inserted++;
        return inserted;
    }

    /**
     * Inserts a list of MeterReading objects into the database in batches.
     * Uses MERGE INTO for H2 and ON CONFLICT for PostgreSQL for idempotency.
     * @param readings List of MeterReading objects to insert
     * @param auditLogger Consumer for audit log messages
     * @return Number of rows inserted
     * @throws SQLException if a database error occurs
     */
    public int insertReadings(List<MeterReading> readings, Consumer<String> auditLogger) throws SQLException {
        boolean isH2 = dbUrl != null && dbUrl.contains("h2:");
        String sql = isH2
            ? "MERGE INTO meter_readings (nmi, timestamp, consumption) KEY (nmi, timestamp) VALUES (?, ?, ?)"
            : "INSERT INTO meter_readings (nmi, timestamp, consumption) VALUES (?, ?, ?) ON CONFLICT (nmi, timestamp) DO NOTHING";
        int inserted = 0, count = 0;
        try (Connection conn = DriverManager.getConnection(dbUrl, dbUser, dbPassword);
             PreparedStatement ps = conn.prepareStatement(sql)) {
            for (MeterReading reading : readings) {
                // Set parameters for each reading
                ps.setString(1, reading.getNmi());
                ps.setObject(2, reading.getTimestamp());
                ps.setDouble(3, reading.getConsumption());
                ps.addBatch();
                if (++count % batchSize == 0) inserted += executeBatchAndCount(ps);
            }
            // Execute any remaining batch
            inserted += executeBatchAndCount(ps);
        }
        auditLogger.accept("Rows inserted: " + inserted);
        return inserted;
    }
}
