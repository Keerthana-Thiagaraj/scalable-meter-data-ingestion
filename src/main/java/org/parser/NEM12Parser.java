package org.parser;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

/**
 * NEM12Parser parses NEM12 files line-by-line, validates structure and schema,
 * and streams valid MeterReading objects to a consumer. Errors are logged with context.
 */
public class NEM12Parser implements FileParser {
    // Record type constants
    private static final String RECORD_TYPE_HEADER = "100";
    private static final String RECORD_TYPE_NMI = "200";
    private static final String RECORD_TYPE_INTERVAL = "300";
    private static final String RECORD_TYPE_IGNORE = "500";
    private static final String RECORD_TYPE_END = "900";

    // Error message constants
    private static final String ERR_HEADER_NOT_FIRST = "100 record must be first";
    private static final String ERR_200_FIELDS = "Insufficient fields in 200 record";
    private static final String ERR_300_FIELDS = "Insufficient fields in 300 record";
    private static final String ERR_300_NO_NMI = "No NMI context for 300 record";
    private static final String ERR_300_DATE = "Invalid date in 300 record";
    private static final String ERR_300_INTERVALS = "Interval count mismatch: expected ";
    private static final String ERR_300_NON_NUMERIC = "Non-numeric consumption value";
    private static final String ERR_200_INTERVAL_LENGTH = "Invalid interval length in 200 record";
    private static final String ERR_UNKNOWN_RECORD = "Unknown record type";
    private static final String ERR_FILE_STRUCTURE = "File missing valid start (100) or end (900) record";

    private final ErrorLogger errorLogger;

    /**
     * @param errorLogger ErrorLogger instance for logging errors
     */
    public NEM12Parser(ErrorLogger errorLogger) {
        this.errorLogger = errorLogger;
    }

    /**
     * Logs an error and returns true for error counting.
     */
    private boolean logErrorAndContinue(String fileName, int lineNumber, String recordType, String reason) {
        errorLogger.logError(fileName, lineNumber, recordType, reason);
        return true;
    }

    /**
     * Handles header record validation.
     */
    private boolean handleHeaderRecord(int lineNumber, String fileName, String recordType) {
        if (lineNumber != 1) {
            return logErrorAndContinue(fileName, lineNumber, recordType, ERR_HEADER_NOT_FIRST);
        }
        return false;
    }

    /**
     * Handles NMI record validation and context extraction.
     */
    private boolean handleNmiRecord(String[] fields, int lineNumber, String fileName, String recordType) {
        if (fields.length < 3) {
            logErrorAndContinue(fileName, lineNumber, recordType, ERR_200_FIELDS);
            return false;
        }
        return true;
    }

    /**
     * Handles interval record validation and reading extraction.
     */
    private int handleIntervalRecord(String[] fields, int lineNumber, String fileName, String recordType, String currentNmi, int expectedIntervals, Consumer<MeterReading> readingConsumer) {
        int errorRows = 0;
        if (fields.length < 3) {
            errorRows += logErrorAndContinue(fileName, lineNumber, recordType, ERR_300_FIELDS) ? 1 : 0;
            return errorRows;
        }
        if (currentNmi == null) {
            errorRows += logErrorAndContinue(fileName, lineNumber, recordType, ERR_300_NO_NMI) ? 1 : 0;
            return errorRows;
        }
        LocalDate date;
        try {
            date = LocalDate.parse(fields[1].trim());
        } catch (Exception e) {
            errorRows += logErrorAndContinue(fileName, lineNumber, recordType, ERR_300_DATE) ? 1 : 0;
            return errorRows;
        }
        int intervals = fields.length - 2;
        if (intervals != expectedIntervals) {
            errorRows += logErrorAndContinue(fileName, lineNumber, recordType, ERR_300_INTERVALS + expectedIntervals + ", got " + intervals) ? 1 : 0;
            return errorRows;
        }
        for (int i = 0; i < intervals; i++) {
            String value = fields[i + 2].trim();
            if (!isNumeric(value)) {
                errorRows += logErrorAndContinue(fileName, lineNumber, recordType, ERR_300_NON_NUMERIC) ? 1 : 0;
                continue;
            }
            LocalTime time = LocalTime.of((24 * i) / intervals, 0);
            readingConsumer.accept(new MeterReading(currentNmi, LocalDateTime.of(date, time), Double.parseDouble(value)));
        }
        return errorRows;
    }

    /**
     * Parses a NEM12 file, streams valid readings, and logs errors/audit info.
     * @param fileName Path to NEM12 file
     * @param readingConsumer Consumer for valid MeterReading objects
     * @param auditLogger Consumer for audit log messages
     * @throws IOException if file structure is invalid
     */
    public void parse(String fileName, Consumer<MeterReading> readingConsumer, Consumer<String> auditLogger) throws IOException {
        List<MeterReading> insertedReadings = new ArrayList<>();
        int errorRows = 0, lineNumber = 0;
        boolean validStart = false, validEnd = false;
        String currentNmi = null;
        int expectedIntervals = 0;
        try (BufferedReader reader = new BufferedReader(new FileReader(fileName))) {
            String line;
            while ((line = reader.readLine()) != null) {
                lineNumber++;
                String[] fields = line.split(",");
                if (fields.length == 0) continue;
                String recordType = fields[0].trim();
                switch (recordType) {
                    case RECORD_TYPE_HEADER:
                        validStart = true;
                        if (handleHeaderRecord(lineNumber, fileName, recordType)) errorRows++;
                        break;
                    case RECORD_TYPE_NMI:
                        if (handleNmiRecord(fields, lineNumber, fileName, recordType)) {
                            currentNmi = fields[1].trim();
                            try {
                                expectedIntervals = Integer.parseInt(fields[2].trim());
                            } catch (NumberFormatException e) {
                                errorRows += logErrorAndContinue(fileName, lineNumber, recordType, ERR_200_INTERVAL_LENGTH) ? 1 : 0;
                            }
                        }
                        break;
                    case RECORD_TYPE_INTERVAL:
                        int errors = handleIntervalRecord(fields, lineNumber, fileName, recordType, currentNmi, expectedIntervals, reading -> {
                            readingConsumer.accept(reading);
                            insertedReadings.add(reading);
                        });
                        errorRows += errors;
                        break;
                    case RECORD_TYPE_IGNORE:
                        // 500 records are ignored
                        break;
                    case RECORD_TYPE_END:
                        validEnd = true;
                        break;
                    default:
                        errorRows += logErrorAndContinue(fileName, lineNumber, recordType, ERR_UNKNOWN_RECORD) ? 1 : 0;
                }
            }
            if (!validStart || !validEnd) throw new IOException(ERR_FILE_STRUCTURE);
        }
        auditLogger.accept("File: " + fileName + ", Rows inserted: " + insertedReadings.size() + ", Errors: " + errorRows);
    }

    /**
     * Checks if a string is numeric.
     */
    private boolean isNumeric(String str) {
        try { Double.parseDouble(str); return true; } catch (NumberFormatException e) { return false; }
    }
}
