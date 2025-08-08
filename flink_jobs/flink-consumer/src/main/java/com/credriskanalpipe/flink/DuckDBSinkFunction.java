package com.credriskanalpipe.flink;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.sql.*;

public class DuckDBSinkFunction extends RichSinkFunction<MarketData> {
    private static final Logger LOG = LoggerFactory.getLogger(DuckDBSinkFunction.class);
    private final String dbPath;
    private Connection connection;
    private PreparedStatement insertStatement;

    public DuckDBSinkFunction(String dbPath) { this.dbPath = dbPath; }

    @Override
    public void open(Configuration parameters) {
        try {
            Class.forName("org.duckdb.DuckDBDriver");
            connection = DriverManager.getConnection("jdbc:duckdb:" + dbPath);
            connection.setAutoCommit(true);
            connection.createStatement().executeUpdate(
                "CREATE TABLE IF NOT EXISTS market_data (" +
                "symbol VARCHAR, timestamp VARCHAR, open DOUBLE, high DOUBLE, " +
                "low DOUBLE, close DOUBLE, volume BIGINT)"
            );
            insertStatement = connection.prepareStatement(
                "INSERT INTO market_data (symbol, timestamp, open, high, low, close, volume) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?)"
            );
            try (ResultSet rs = connection.createStatement().executeQuery("PRAGMA database_list")) {
                while (rs.next()) LOG.info("DuckDB attached: name={}, file={}", rs.getString(1), rs.getString(2));
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to init DuckDB sink", e);
        }
    }

    @Override
    public void invoke(MarketData v, Context ctx) {
        if (v == null) return;
        try {
            insertStatement.setString(1, v.symbol);
            insertStatement.setString(2, v.timestamp);
            insertStatement.setDouble(3, v.open);
            insertStatement.setDouble(4, v.high);
            insertStatement.setDouble(5, v.low);
            insertStatement.setDouble(6, v.close);
            insertStatement.setLong(7, v.volume);
            insertStatement.executeUpdate();
        } catch (SQLException e) {
            LOG.error("Insert failed: {}", v, e);
        }
    }

    @Override
    public void close() {
        try { if (insertStatement != null) insertStatement.close(); } catch (SQLException ignored) {}
        try { if (connection != null && !connection.isClosed()) connection.close(); } catch (SQLException ignored) {}
    }
}
