package com.credriskanalpipe.flink;

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
        env.setParallelism(1); // <-- must come after the line above

        KafkaSource<MarketData> source = KafkaSource.<MarketData>builder()
                .setBootstrapServers("broker:29092")               // internal Kafka listener
                .setTopics("market_data")
                .setGroupId("flink-market-consumer")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new MarketDataDeserializationSchema())
                .build();

        DataStream<MarketData> stream =
                env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source")
                .map(m -> { LOG.info("Got: {}", m); return m; });

        String duckDbPath = System.getenv().getOrDefault(
                "WAREHOUSE_PATH", "/mnt/duckdb/warehouse.duckdb");

        stream.addSink(new DuckDBSinkFunction(duckDbPath)).name("duckdb-sink");

        env.execute("Kafka Market Data Consumer with DuckDB Sink");
    }

}