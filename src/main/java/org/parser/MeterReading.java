package org.parser;

import java.time.LocalDateTime;

/**
 * MeterReading represents a single meter reading record with NMI, timestamp, and consumption value.
 */
public class MeterReading {
    private final String nmi;
    private final LocalDateTime timestamp;
    private final double consumption;

    /**
     * @param nmi National Metering Identifier
     * @param timestamp Date and time of the reading
     * @param consumption Consumption value for the interval
     */
    public MeterReading(String nmi, LocalDateTime timestamp, double consumption) {
        this.nmi = nmi;
        this.timestamp = timestamp;
        this.consumption = consumption;
    }

    /**
     * @return NMI for this reading
     */
    public String getNmi() { return nmi; }
    /**
     * @return Timestamp for this reading
     */
    public LocalDateTime getTimestamp() { return timestamp; }
    /**
     * @return Consumption value for this reading
     */
    public double getConsumption() { return consumption; }
}
