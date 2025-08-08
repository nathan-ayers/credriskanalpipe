package com.credriskanalpipe.flink;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.charset.StandardCharsets;

public class MarketDataDeserializationSchema implements DeserializationSchema<MarketData> {
    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(MarketDataDeserializationSchema.class);
    private transient ObjectMapper mapper;

    @Override
    public void open(InitializationContext context) {
        mapper = new ObjectMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    }

    @Override
    public MarketData deserialize(byte[] message) {
        if (message == null) return null;
        String s = new String(message, StandardCharsets.UTF_8).trim();
        if (s.isEmpty()) return null;
        try { return mapper.readValue(s, MarketData.class); }
        catch (Exception e) { LOG.warn("Skipping bad record: {}", s, e); return null; }
    }

    @Override public boolean isEndOfStream(MarketData nextElement) { return false; }
    @Override public TypeInformation<MarketData> getProducedType() { return TypeInformation.of(MarketData.class); }
}
