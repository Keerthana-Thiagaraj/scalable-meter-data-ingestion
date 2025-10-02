package org.parser;

import java.io.IOException;
import java.util.function.Consumer;

public class Main {
    public static void main(String[] args) {
        String fileName = "test_nem12.csv"; // Change to your input file
        ErrorLogger errorLogger = new ErrorLogger("test_error_log.csv");
        int intervalLength = 48; // Example interval length, adjust as needed
        FileParser parser = ParserFactory.getParser("NEM12", errorLogger, intervalLength);

        Consumer<MeterReading> readingConsumer = reading -> {
            System.out.println("Reading: " + reading);
        };
        Consumer<String> auditLogger = audit -> {
            System.out.println("Audit: " + audit);
        };

        try {
            parser.parse(fileName, readingConsumer, auditLogger);
        } catch (IOException e) {
            System.err.println("Error parsing file: " + e.getMessage());
        }
    }
}
