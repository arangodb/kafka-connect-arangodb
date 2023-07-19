package com.arangodb.kafka.utils;

import com.arangodb.ArangoCollection;

import java.time.Duration;
import java.util.AbstractMap;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.awaitility.Awaitility.await;

public final class Utils {
    private Utils() {
    }

    public static Map.Entry<Object, Map<String, Object>> record(Object key, Map<String, Object> value) {
        return new AbstractMap.SimpleEntry<>(key, value);
    }

    public static FluentMap<String, Object> map() {
        return new FluentMap<>();
    }

    public static void awaitCount(ArangoCollection col, int count) {
        await().atMost(Duration.ofSeconds(60))
                .until(() -> col.count().getCount() >= count);
    }

    public static class FluentMap<K, V> extends LinkedHashMap<K, V> {
        public FluentMap<K, V> add(K key, V value) {
            super.put(key, value);
            return this;
        }
    }
}
