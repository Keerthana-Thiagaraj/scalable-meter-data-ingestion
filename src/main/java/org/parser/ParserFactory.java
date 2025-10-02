package org.parser;

public class ParserFactory {
    public static FileParser getParser(String fileType, ErrorLogger errorLogger, int intervalLength) {
        switch (fileType) {
            case "NEM12":
                return new NEM12Parser(errorLogger, intervalLength);
            // case "NEM13":
            //     return new NEM13Parser(errorLogger, intervalLength);
            default:
                throw new IllegalArgumentException("Unknown file type: " + fileType);
        }
    }
}

