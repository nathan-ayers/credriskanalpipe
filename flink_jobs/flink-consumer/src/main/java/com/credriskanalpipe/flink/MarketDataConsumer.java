package com.credriskanalpipe.flink;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
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

        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers("broker:29092")
                .setTopics("raw_market_ohlcv")
                .setGroupId("flink-market-consumer-group")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStream<String> kafkaStream = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");

        // Add a map function to explicitly log and handle potential deserialization issues
        DataStream<String> processedStream = kafkaStream.map(message -> {
            try {
                // Here, you could add JSON parsing logic if needed
                // For now, we just log the raw message to confirm receipt
                LOG.info("Successfully received message from Kafka: {}", message);
                return message; // Pass the message through
            } catch (Exception e) {
                LOG.error("Failed to process message: {}", message, e);
                return null; // Filter out messages that cause errors
            }
        }).filter(message -> message != null);

        // Sink to logs (equivalent to .print() but with explicit logging)
        processedStream.addSink(new org.apache.flink.streaming.api.functions.sink.PrintSinkFunction<>());

        LOG.info("Executing Flink job...");
        env.execute("Kafka Market Data Consumer with Logging");
    }
}