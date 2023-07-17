package com.arangodb.kafka.target.ssl;

import com.arangodb.kafka.target.TargetHolder;
import com.arangodb.kafka.target.TestTarget;

public enum SslTargets implements TargetHolder {
    SslTarget(SslTarget.class),
    SslFromFileTarget(SslFromFileTarget.class);

    private final Class<? extends TestTarget> clazz;

    SslTargets(final Class<? extends TestTarget> clazz) {
        this.clazz = clazz;
    }

    @Override
    public Class<? extends TestTarget> getClazz() {
        return clazz;
    }
}
