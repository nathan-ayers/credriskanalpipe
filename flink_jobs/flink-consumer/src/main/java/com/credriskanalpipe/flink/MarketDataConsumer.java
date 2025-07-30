package com.credriskanalpipe.flink;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.common.serialization.StringDeserializer; // CORRECT IMPORT

public class MarketDataConsumer {
    public static void main(String[] args) throws Exception {
        // 1. Set up the streaming execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 2. Configure KafkaSource to consume from your raw_market_ohlcv topic
        KafkaSource<String> source = KafkaSource.<String>builder()
            .setBootstrapServers("broker:29092")
            .setTopics("raw_market_ohlcv")
            .setGroupId("flink-market-consumer-group")
            .setStartingOffsets(OffsetsInitializer.earliest())
            // IMPORTANT: Use the Flink helper method that takes the Kafka Deserializer class
            .setDeserializer(KafkaRecordDeserializationSchema.valueOnly(StringDeserializer.class)) // Preferred for String
            // OR if you really want to use Kafka's StringDeserializer directly (less common with SimpleStringSchema available):
            // .setValueOnlyDeserializer(new org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema.of(StringDeserializer.class))
            .build();

        // 3. Add the Kafka source to the Flink environment
        env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source")
            // 4. Print each message to standard output (for verification)
            .print();

        // 5. Execute the Flink job
        env.execute("Kafka Market Data Consumer");
    }
}
