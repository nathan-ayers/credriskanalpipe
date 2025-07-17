package com.credriskanalpipe.flink; // This MUST match your folder structure and groupId

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.datastream.DataStream;

public class StreamingJob {

    public static void main(String[] args) throws Exception {
        // Set up the streaming execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Configure Kafka consumer
        // Use the Docker internal service name 'broker' and the internal port 29092
        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers("broker:29092")
                .setTopics("raw_events") // This is the topic we created in Kafka
                .setGroupId("my-flink-consumer-group")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        // Create a Flink DataStream from Kafka
        DataStream<String> kafkaStream = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");

        // Perform a simple transformation (e.g., print to console for now)
        kafkaStream.map(value -> "Received: " + value.toUpperCase())
                   .print(); // This will print to the Flink TaskManager logs

        // Execute the Flink job
        env.execute("Flink Kafka Consumer Job");
    }
}