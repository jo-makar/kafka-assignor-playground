package org.example;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.kafka.common.serialization.Deserializer;

import java.io.IOException;
import java.util.logging.Logger;
import java.util.Map;

public class JsonDeserializer implements Deserializer<Map<String,Object>> {
    private static Logger logger = Logger.getLogger(JsonDeserializer.class.getName());
    private final ObjectMapper mapper = new ObjectMapper();

    @Override public Map<String,Object> deserialize(String topic, byte[] data) {
        if (data == null || data.length == 0)
            return null;

        try {
            return mapper.readValue(data, new TypeReference<Map<String,Object>>() {});
        } catch (IOException e) {
            logger.warning("Failed to deserialize data: " + e.getMessage());
            return null;
        }
    }
}
