package com.credriskanalpipe.flink;

import com.credriskanalpipe.flink.DuckDBSinkFunction;
import com.credriskanalpipe.flink.MarketData;
import com.credriskanalpipe.flink.MarketDataDeserializationSchema; // <-- NEW: Import your custom deserializer

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
// import org.apache.flink.api.common.serialization.SimpleStringSchema; // <-- REMOVE or comment out
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MarketDataConsumer {

    private static final Logger LOG = LoggerFactory.getLogger(MarketDataConsumer.class);

    public static void main(String[] args) throws Exception {
        LOG.info("Starting Flink Kafka Consumer Job...");

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // --- CORRECTED KAFKA SOURCE TO USE MARKETDATA DESERIALIZER ---
        KafkaSource<MarketData> source = KafkaSource.<MarketData>builder() // <-- Change <String> to <MarketData>
                .setBootstrapServers("broker:9092") // Assuming this is correct from docker-compose
                .setTopics("market_data") // Assuming this is your topic name
                .setGroupId("flink-market-consumer-group")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new MarketDataDeserializationSchema()) // <-- NEW: Use your custom deserializer
                .build();

        // --- kafkaStream is now DataStream<MarketData> ---
        DataStream<MarketData> kafkaStream = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");

        // --- Remove the unnecessary .map and .filter, as deserialization is handled by the source ---
        DataStream<MarketData> processedStream = kafkaStream.map(marketData -> {
            // This map function can be used for logging or further processing of MarketData objects
            LOG.info("Successfully received and parsed message from Kafka: {}", marketData);
            return marketData;
        }).filter(marketData -> marketData != null); // Keep the filter if you want to filter nulls

        // Sink to DuckDB
        String duckDbPath = "/mnt/duckdb/warehouse.duckdb"; // Path within the Flink container
        processedStream.addSink(new DuckDBSinkFunction(duckDbPath));

        LOG.info("Executing Flink job...");
        env.execute("Kafka Market Data Consumer with DuckDB Sink"); // Changed job name for clarity
    }
}