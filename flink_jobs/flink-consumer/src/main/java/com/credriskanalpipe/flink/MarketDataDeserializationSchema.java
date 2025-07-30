package com.credriskanalpipe.flink;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation; // Required for getProducedType
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;

public class MarketDataDeserializationSchema implements DeserializationSchema<MarketData> {

    private static final long serialVersionUID = 1L;
    private transient ObjectMapper objectMapper;

    @Override
    public void open(DeserializationSchema.InitializationContext context) throws Exception {
        objectMapper = new ObjectMapper();
    }

    @Override
    public MarketData deserialize(byte[] message) throws IOException {
        return objectMapper.readValue(message, MarketData.class);
    }

    // Correct implementation of isEndOfStream
    @Override
    public boolean isEndOfStream(MarketData nextElement) {
        return false; // We are processing a continuous stream, so it's never ending.
    }

    // Correct implementation of getProducedType
    @Override
    public TypeInformation<MarketData> getProducedType() {
        return TypeInformation.of(MarketData.class);
    }
}