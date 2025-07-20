package org.example;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import java.util.List;
import java.util.Map;

class JsonDeserializerTest {
    @Test void testBasic() {
        try (JsonDeserializer deserializer = new JsonDeserializer()) {
            assertNull(deserializer.deserialize("topic", null));
            assertNull(deserializer.deserialize("topic", new byte[0]));

            assertNull(deserializer.deserialize("topic", "1".getBytes()));
            assertNull(deserializer.deserialize("topic", "[]".getBytes()));

            assertNull(deserializer.deserialize("topic", "{".getBytes()));

            assertEquals(Map.of(), deserializer.deserialize("topic", "{}".getBytes()));
            assertEquals(Map.of("a", 1), deserializer.deserialize("topic", "{\"a\":1}".getBytes()));
            assertEquals(Map.of("a", "s"), deserializer.deserialize("topic", "{\"a\":\"s\"}".getBytes()));
            assertEquals(Map.of("a", true), deserializer.deserialize("topic", "{\"a\":true}".getBytes()));
            assertEquals(Map.of("a", 1.5), deserializer.deserialize("topic", "{\"a\":1.5}".getBytes()));
            assertEquals(Map.of("a", List.of(1)), deserializer.deserialize("topic", "{\"a\":[1]}".getBytes()));
            assertEquals(Map.of("a", Map.of("b", 1)), deserializer.deserialize("topic", "{\"a\":{\"b\":1}}".getBytes()));

            assertEquals(
                Map.of("special", "!@#$%^&*()", "escaped", "\"quoted\""),
                deserializer.deserialize("topic", "{\"special\":\"!@#$%^&*()\",\"escaped\":\"\\\"quoted\\\"\"}".getBytes())
            );
        }
    }

    @Test void testUnicode() {
        try (JsonDeserializer deserializer = new JsonDeserializer()) {
            assertEquals(
                // 世界 (sekai, world): こんにちは (konnichiwa, hello)
                Map.of("世界", "こんにちは"),
                deserializer.deserialize("topic", "{\"世界\":\"こんにちは\"}".getBytes())
            );
        }
    }
}
