package com.arangodb.kafka.utils;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.LoggerContext;
import com.arangodb.ArangoCollection;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.AbstractMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.IntPredicate;

import static org.awaitility.Awaitility.await;

public final class Utils {
    public static final int TESTS_TIMEOUT_SECONDS = 120;

    private Utils() {
    }

    public static Map.Entry<Object, Map<String, Object>> record(Object key, Map<String, Object> value) {
        return new AbstractMap.SimpleEntry<>(key, value);
    }

    public static FluentMap<String, Object> map() {
        return new FluentMap<>();
    }

    public static void awaitCount(ArangoCollection col, int count) {
        awaitCount(col, gte(count));
    }

    public static void awaitCount(ArangoCollection col, IntPredicate condition) {
        await().atMost(Duration.ofSeconds(TESTS_TIMEOUT_SECONDS))
                .until(() -> condition.test(col.count().getCount().intValue()));
    }

    public static void awaitKey(ArangoCollection col, String key) {
        await().atMost(Duration.ofSeconds(TESTS_TIMEOUT_SECONDS))
                .until(() -> col.documentExists(key));
    }

    public static void awaitDlq(Map<String, ?> dlq, int count) {
        await().atMost(Duration.ofSeconds(TESTS_TIMEOUT_SECONDS))
                .until(() -> dlq.size() >= count);
    }

    public static IntPredicate eq(int v) {
        return x -> x == v;
    }

    public static IntPredicate gte(int v) {
        return x -> x >= v;
    }

    public static MemoryAppender interceptLogger(Class<?> name) {
        Logger logger = (Logger) LoggerFactory.getLogger(name);
        MemoryAppender memoryAppender = new MemoryAppender();
        memoryAppender.setContext((LoggerContext) LoggerFactory.getILoggerFactory());
        logger.addAppender(memoryAppender);
        memoryAppender.start();
        return memoryAppender;
    }

    public static class FluentMap<K, V> extends LinkedHashMap<K, V> {
        public FluentMap<K, V> add(K key, V value) {
            super.put(key, value);
            return this;
        }
    }
}
