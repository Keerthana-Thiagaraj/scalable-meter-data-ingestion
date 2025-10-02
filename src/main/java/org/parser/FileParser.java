package org.parser;

import java.io.IOException;
import java.util.function.Consumer;

public interface FileParser {
    void parse(String fileName, Consumer<MeterReading> readingConsumer, Consumer<String> auditLogger) throws IOException;
}

