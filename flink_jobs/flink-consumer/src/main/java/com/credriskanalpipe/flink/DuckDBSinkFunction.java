package com.credriskanalpipe.flink;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class DuckDBSinkFunction extends RichSinkFunction<MarketData> {

    private static final Logger LOG = LoggerFactory.getLogger(DuckDBSinkFunction.class);
    private String dbPath;
    private Connection connection;
    private PreparedStatement insertStatement;

    public DuckDBSinkFunction(String dbPath) {
        this.dbPath = dbPath;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        LOG.info("Opening DuckDBSinkFunction. DB Path: {}", dbPath);
        try {
            // Load the DuckDB JDBC driver
            Class.forName("org.duckdb.DuckDBDriver");
            // Establish connection
            connection = DriverManager.getConnection("jdbc:duckdb:" + dbPath);
            connection.setAutoCommit(true); // Auto-commit for simplicity, consider batching for performance

            // Create table if it does not exist
            // IMPORTANT: Ensure this schema matches your MarketData POJO's fields and types
            String createTableSQL = "CREATE TABLE IF NOT EXISTS market_data (" +
                                    "symbol VARCHAR, " +
                                    "timestamp VARCHAR, " + // Matches MarketData.java
                                    "open DOUBLE, " +
                                    "high DOUBLE, " +
                                    "low DOUBLE, " +
                                    "close DOUBLE, " +
                                    "volume BIGINT" +
                                    ");";
            connection.createStatement().executeUpdate(createTableSQL);
            LOG.info("DuckDB table 'market_data' checked/created successfully.");

            // Prepare the insert statement
            insertStatement = connection.prepareStatement(
                "INSERT INTO market_data (symbol, timestamp, open, high, low, close, volume) VALUES (?, ?, ?, ?, ?, ?, ?);"
            );
            LOG.info("DuckDB insert statement prepared.");

        } catch (ClassNotFoundException e) {
            LOG.error("DuckDB JDBC Driver not found!", e);
            throw new RuntimeException("DuckDB JDBC Driver not found", e);
        } catch (SQLException e) {
            LOG.error("SQL Exception during DuckDB connection or table creation: {}", e.getMessage(), e);
            throw new RuntimeException("Failed to connect to DuckDB or create table", e);
        }
    }

    @Override
    public void invoke(MarketData value, Context context) throws Exception {
        try {
            insertStatement.setString(1, value.symbol);
            insertStatement.setString(2, value.timestamp); // Matches MarketData.java
            insertStatement.setDouble(3, value.open);
            insertStatement.setDouble(4, value.high);
            insertStatement.setDouble(5, value.low);
            insertStatement.setDouble(6, value.close);
            insertStatement.setLong(7, value.volume);
            insertStatement.executeUpdate();
            // LOG.info("Inserted MarketData: {}", value.symbol); // Uncomment for verbose logging
        } catch (SQLException e) {
            LOG.error("SQL Exception during data insertion: {} - Data: {}", e.getMessage(), value.toString(), e);
        }
    }

    @Override
    public void close() throws Exception {
        super.close();
        LOG.info("Closing DuckDBSinkFunction.");
        try {
            if (insertStatement != null) {
                insertStatement.close();
            }
            if (connection != null && !connection.isClosed()) {
                connection.close();
            }
        } catch (SQLException e) {
            LOG.error("Error closing DuckDB connection or statement: {}", e.getMessage(), e);
        }
    }
}