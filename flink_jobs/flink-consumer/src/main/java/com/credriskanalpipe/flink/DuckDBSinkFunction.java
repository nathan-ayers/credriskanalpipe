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

    public DuckDBSinkFunction(String dbPath) {
        this.dbPath = dbPath;
    }

    @Override
    public void open(Configuration parameters) {
        try {
            Class.forName("org.duckdb.DuckDBDriver");
            connection = DriverManager.getConnection("jdbc:duckdb:" + dbPath);
            connection.setAutoCommit(true);

            // Ensure table exists
            try (Statement st = connection.createStatement()) {
                st.executeUpdate(
                    "CREATE TABLE IF NOT EXISTS market_data (" +
                    "  symbol VARCHAR," +
                    "  timestamp VARCHAR," +    // stored as string per your POJO
                    "  open DOUBLE, high DOUBLE, low DOUBLE, close DOUBLE, volume BIGINT" +
                    ")"
                );
            }

            // Create unique index, auto-dedupe once if it fails
            ensureUniqueIndexWithAutoDedup();

            // Idempotent insert
            insertStatement = connection.prepareStatement(
                "INSERT INTO market_data (symbol, timestamp, open, high, low, close, volume) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?) " +
                "ON CONFLICT (symbol, timestamp) DO NOTHING"
            );

            // Log attached DB file for sanity
            try (ResultSet rs = connection.createStatement().executeQuery("PRAGMA database_list")) {
                while (rs.next()) {
                    LOG.info("DuckDB attached: name={}, file={}", rs.getString(1), rs.getString(2));
                }
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to init DuckDB sink", e);
        }
    }

    // <<< this was the missing/corrupted signature >>>
    private void ensureUniqueIndexWithAutoDedup() throws SQLException {
        try (Statement st = connection.createStatement()) {
            try {
                // Some DuckDB builds don't accept IF NOT EXISTS here — so use plain CREATE and handle errors.
                st.executeUpdate(
                    "CREATE UNIQUE INDEX ux_market_data ON market_data(symbol, timestamp)"
                );
                LOG.info("Created unique index ux_market_data on (symbol, timestamp).");
            } catch (SQLException e) {
                final String msg = (e.getMessage() == null ? "" : e.getMessage().toLowerCase());
                if (msg.contains("already exists")) {
                    // Index is already there — all good.
                    LOG.info("Unique index ux_market_data already exists; continuing.");
                } else if (msg.contains("duplicate") || msg.contains("constraint")) {
                    // Data has dup keys; dedupe once, then (re)create index.
                    LOG.warn("Duplicates detected while creating unique index; performing one-time dedupe…");

                    st.executeUpdate(
                        "CREATE TEMPORARY TABLE _md_dedup AS " +
                        "SELECT symbol, timestamp, open, high, low, close, volume " +
                        "FROM (" +
                        "  SELECT *, ROW_NUMBER() OVER (PARTITION BY symbol, timestamp ORDER BY timestamp) rn " +
                        "  FROM market_data" +
                        ") WHERE rn = 1"
                    );
                    st.executeUpdate("DELETE FROM market_data");
                    st.executeUpdate("INSERT INTO market_data SELECT * FROM _md_dedup");
                    st.executeUpdate("DROP TABLE _md_dedup");

                    // Try to create index again; ignore 'already exists'.
                    try {
                        st.executeUpdate(
                            "CREATE UNIQUE INDEX ux_market_data ON market_data(symbol, timestamp)"
                        );
                    } catch (SQLException e2) {
                        final String msg2 = (e2.getMessage() == null ? "" : e2.getMessage().toLowerCase());
                        if (!msg2.contains("already exists")) throw e2;
                    }
                    LOG.info("Dedupe complete; unique index ensured.");
                } else {
                    throw e;
                }
            }
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
            final String msg = e.getMessage() == null ? "" : e.getMessage().toLowerCase();
            if (msg.contains("constraint") || msg.contains("unique")) {
                // duplicate -> safely ignore
                return;
            }
            LOG.error("Insert failed: {}", v, e);
        }
    }

    @Override
    public void close() {
        try { if (insertStatement != null) insertStatement.close(); } catch (SQLException ignore) {}
        try { if (connection != null && !connection.isClosed()) connection.close(); } catch (SQLException ignore) {}
    }
}
