package com.arangodb.kafka.target.write;

import com.arangodb.kafka.target.converter.JsonTarget;

abstract class AbstractWriteTarget extends JsonTarget {

    public AbstractWriteTarget(String name) {
        super(name);
    }

    @Override
    public int getTopicPartitions() {
        return 1;
    }
}
