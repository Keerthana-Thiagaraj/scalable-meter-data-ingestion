package org.parser;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;

/**
 * ErrorLogger writes error details to a CSV file for audit and debugging.
 */
public class ErrorLogger {
    private final String errorFile;

    /**
     * @param errorFile Path to the error log file
     */
    public ErrorLogger(String errorFile) {
        this.errorFile = errorFile;
    }

    /**
     * Logs an error with file name, line number, record type, and reason.
     * Each error is appended as a CSV line.
     */
    public void logError(String fileName, int lineNumber, String recordType, String reason) {
        String errorMsg = fileName + "," + lineNumber + "," + recordType + "," + reason;
        try (PrintWriter out = new PrintWriter(new FileWriter(errorFile, true))) {
            out.println(errorMsg);
        } catch (IOException e) {
            e.printStackTrace();
        }
        // Print error to terminal as well
        System.err.println("Error: " + errorMsg);
    }
}
