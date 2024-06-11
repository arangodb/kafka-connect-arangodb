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

    /**
     * Parses {@param version} and checks whether it is greater or equal to <{@param otherMajor}, {@param otherMinor},
     * {@param otherPatch}> comparing the corresponding version components in lexicographical order.
     */
    public static boolean isAtLeastVersion(final String version, final int otherMajor, final int otherMinor,
                                           final int otherPatch) {
        return compareVersion(version, otherMajor, otherMinor, otherPatch) >= 0;
    }

    /**
     * Parses {@param version} and checks whether it is less than <{@param otherMajor}, {@param otherMinor},
     * {@param otherPatch}> comparing the corresponding version components in lexicographical order.
     */
    public static boolean isLessThanVersion(final String version, final int otherMajor, final int otherMinor,
                                            final int otherPatch) {
        return compareVersion(version, otherMajor, otherMinor, otherPatch) < 0;
    }

    private static int compareVersion(final String version, final int otherMajor, final int otherMinor,
                                      final int otherPatch) {
        String[] parts = version.split("-")[0].split("\\.");

        int major = Integer.parseInt(parts[0]);
        int minor = Integer.parseInt(parts[1]);
        int patch = Integer.parseInt(parts[2]);

        int majorComparison = Integer.compare(major, otherMajor);
        if (majorComparison != 0) {
            return majorComparison;
        }

        int minorComparison = Integer.compare(minor, otherMinor);
        if (minorComparison != 0) {
            return minorComparison;
        }

        return Integer.compare(patch, otherPatch);
    }
}
