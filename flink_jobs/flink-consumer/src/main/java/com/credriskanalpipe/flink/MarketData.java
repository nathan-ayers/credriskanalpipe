package com.credriskanalpipe.flink;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

// Using @JsonIgnoreProperties to handle potential extra fields in JSON
// without causing deserialization errors.
@JsonIgnoreProperties(ignoreUnknown = true)
public class MarketData {
    public String symbol;
    public String timestamp; // Keep as String to match Kafka producer's JSON
    public double open;
    public double high;
    public double low;
    public double close;
    public long volume;

    // Default constructor is required for Flink's POJO serializer
    public MarketData() {}

    public MarketData(String symbol, String timestamp, double open, double high, double low, double close, long volume) {
        this.symbol = symbol;
        this.timestamp = timestamp;
        this.open = open;
        this.high = high;
        this.low = low;
        this.close = close;
        this.volume = volume;
    }

    @Override
    public String toString() {
        return "MarketData{" +
               "symbol='" + symbol + '\'' +
               ", timestamp='" + timestamp + '\'' +
               ", open=" + open +
               ", high=" + high +
               ", low=" + low +
               ", close=" + close +
               ", volume=" + volume +
               '}';
    }
}