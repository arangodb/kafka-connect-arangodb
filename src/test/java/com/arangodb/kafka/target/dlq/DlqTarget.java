package com.arangodb.kafka.target.dlq;

import com.arangodb.kafka.target.write.BaseWriteTarget;

public class DlqTarget extends BaseWriteTarget {
    public DlqTarget(String name) {
        super(name);
    }

    @Override
    public int getTopicPartitions() {
        return 1;
    }

    @Override
    public boolean supportsDLQ() {
        return true;
    }
}
