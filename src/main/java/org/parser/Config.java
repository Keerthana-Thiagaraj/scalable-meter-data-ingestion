package org.parser;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * Flo Energy Tech Assessment - Scalability & Retention
 *
 * - This code does NOT flush or delete readings. All inserts are idempotent and historical data is retained.
 * - For production scalability, partition the meter_readings table by month or year in PostgreSQL.
 *   Example migration script:
 *
 *   CREATE TABLE meter_readings (
 *     id uuid DEFAULT gen_random_uuid() NOT NULL,
 *     nmi varchar(10) NOT NULL,
 *     timestamp timestamp NOT NULL,
 *     consumption numeric NOT NULL,
 *     CONSTRAINT meter_readings_pk PRIMARY KEY (id),
 *     CONSTRAINT meter_readings_unique_consumption UNIQUE (nmi, timestamp)
 *   ) PARTITION BY RANGE (timestamp);
 *
 *   -- Create monthly partitions (example for 2025)
 *   CREATE TABLE meter_readings_2025_01 PARTITION OF meter_readings
 *     FOR VALUES FROM ('2025-01-01') TO ('2025-02-01');
 *   CREATE TABLE meter_readings_2025_02 PARTITION OF meter_readings
 *     FOR VALUES FROM ('2025-02-01') TO ('2025-03-01');
 *   -- Repeat for each month/year as needed
 *
 * - Archival policies for very old data are out of scope for this codebase.
 */
public class Config {
    private static final Properties properties = new Properties();

    static {
        try (InputStream input = Config.class.getClassLoader().getResourceAsStream("config.properties")) {
            if (input != null) {
                properties.load(input);
            } else {
                throw new RuntimeException("config.properties file not found in resources");
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to load config.properties", e);
        }
    }

    public static final int DEFAULT_BATCH_SIZE = Integer.parseInt(properties.getProperty("DEFAULT_BATCH_SIZE", "500"));
    public static final int DEFAULT_INTERVAL_LENGTH = Integer.parseInt(properties.getProperty("DEFAULT_INTERVAL_LENGTH", "48"));
    public static final String DB_URL = properties.getProperty("DB_URL");
    public static final String DB_USER = properties.getProperty("DB_USER");
    public static final String DB_PASSWORD = properties.getProperty("DB_PASSWORD");
    public static final String ERROR_FILE = properties.getProperty("ERROR_FILE", "error_log.csv");
    /**
     * Partitioning guidance:
     * For production scalability, partition the meter_readings table by month or year in PostgreSQL.
     * See class-level comment for example SQL.
     */
}
