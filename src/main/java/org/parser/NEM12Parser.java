package org.parser;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.function.Consumer;

/**
 * NEM12Parser parses NEM12 files line-by-line, validates structure and schema,
 * and streams valid MeterReading objects to a consumer. Errors are logged with context.
 */
public class NEM12Parser {
    private final ErrorLogger errorLogger;
    private final int intervalLength;

    /**
     * @param errorLogger ErrorLogger instance for logging errors
     * @param intervalLength Expected interval count per 200 record
     */
    public NEM12Parser(ErrorLogger errorLogger, int intervalLength) {
        this.errorLogger = errorLogger;
        this.intervalLength = intervalLength;
    }

    /**
     * Logs an error and returns true for error counting.
     */
    private boolean logErrorAndContinue(String fileName, int lineNumber, String recordType, String reason) {
        errorLogger.logError(fileName, lineNumber, recordType, reason);
        return true;
    }

    /**
     * Parses a NEM12 file, streams valid readings, and logs errors/audit info.
     * @param fileName Path to NEM12 file
     * @param readingConsumer Consumer for valid MeterReading objects
     * @param auditLogger Consumer for audit log messages
     * @throws IOException if file structure is invalid
     */
    public void parse(String fileName, Consumer<MeterReading> readingConsumer, Consumer<String> auditLogger) throws IOException {
        int totalRows = 0, errorRows = 0, lineNumber = 0;
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
                    case "100":
                        // Header record, must be first
                        if (lineNumber != 1) errorRows += logErrorAndContinue(fileName, lineNumber, recordType, "100 record must be first") ? 1 : 0;
                        validStart = true;
                        break;
                    case "200":
                        // NMI record, sets context and expected intervals
                        if (fields.length < 3) {
                            errorRows += logErrorAndContinue(fileName, lineNumber, recordType, "Insufficient fields in 200 record") ? 1 : 0;
                            break;
                        }
                        currentNmi = fields[1].trim();
                        // Remove custom missing NMI logic, revert to original
                        try {
                            expectedIntervals = Integer.parseInt(fields[2].trim());
                        } catch (NumberFormatException e) {
                            errorRows += logErrorAndContinue(fileName, lineNumber, recordType, "Invalid interval length in 200 record") ? 1 : 0;
                        }
                        break;
                    case "300":
                        // Interval record, validates and streams readings
                        if (fields.length < 3) { errorRows += logErrorAndContinue(fileName, lineNumber, recordType, "Insufficient fields in 300 record") ? 1 : 0; break; }
                        if (currentNmi == null) { errorRows += logErrorAndContinue(fileName, lineNumber, recordType, "No NMI context for 300 record") ? 1 : 0; break; }
                        LocalDate date;
                        try { date = LocalDate.parse(fields[1].trim()); }
                        catch (Exception e) { errorRows += logErrorAndContinue(fileName, lineNumber, recordType, "Invalid date in 300 record") ? 1 : 0; break; }
                        int intervals = fields.length - 2;
                        if (intervals != expectedIntervals) { errorRows += logErrorAndContinue(fileName, lineNumber, recordType, "Interval count mismatch: expected " + expectedIntervals + ", got " + intervals) ? 1 : 0; break; }
                        for (int i = 0; i < intervals; i++) {
                            String value = fields[i + 2].trim();
                            if (!isNumeric(value)) { errorRows += logErrorAndContinue(fileName, lineNumber, recordType, "Non-numeric consumption value") ? 1 : 0; continue; }
                            LocalTime time = LocalTime.of((24 * i) / intervals, 0);
                            readingConsumer.accept(new MeterReading(currentNmi, LocalDateTime.of(date, time), Double.parseDouble(value)));
                            totalRows++;
                        }
                        break;
                    case "500":
                        // 500 records are ignored
                        break;
                    case "900":
                        // Footer record, must be last
                        validEnd = true;
                        break;
                    default:
                        errorRows += logErrorAndContinue(fileName, lineNumber, recordType, "Unknown record type") ? 1 : 0;
                }
            }
            if (!validStart || !validEnd) throw new IOException("File missing valid start (100) or end (900) record");
        }
        auditLogger.accept("File: " + fileName + ", Rows inserted: " + totalRows + ", Errors: " + errorRows);
    }

    /**
     * Checks if a string is numeric.
     */
    private boolean isNumeric(String str) {
        try { Double.parseDouble(str); return true; } catch (NumberFormatException e) { return false; }
    }
}
