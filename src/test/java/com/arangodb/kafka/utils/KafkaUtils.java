package com.arangodb.kafka.utils;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;

import java.util.HashMap;
import java.util.Map;

public class KafkaUtils {
    private KafkaUtils() {
    }

    public static Map<String, String> extractHeaders(ConsumerRecord<?, ?> record) {
        Map<String, String> res = new HashMap<>();
        for (Header header : record.headers()) {
            res.put(header.key(), new String(header.value()));
        }
        return res;
    }
}
