package com.arangodb.kafka.target.write;

import com.arangodb.kafka.target.converter.JsonTarget;

public class BaseWriteTarget extends JsonTarget {

    public BaseWriteTarget(String name) {
        super(name);
    }

    @Override
    public int getTopicPartitions() {
        return 1;
    }
}
