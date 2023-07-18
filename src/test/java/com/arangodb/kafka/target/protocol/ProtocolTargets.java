package com.arangodb.kafka.target.protocol;

import com.arangodb.kafka.target.TargetHolder;
import com.arangodb.kafka.target.TestTarget;

public enum ProtocolTargets implements TargetHolder {
//    VstTarget(VstTarget.class),
    Http1JsonTarget(Http1JsonTarget.class),
    Http1VpackTarget(Http1VpackTarget.class),
    Http2JsonTarget(Http2JsonTarget.class),
    Http2VpackTarget(Http2VpackTarget.class);

    private final Class<? extends TestTarget> clazz;

    ProtocolTargets(final Class<? extends TestTarget> clazz) {
        this.clazz = clazz;
    }

    @Override
    public Class<? extends TestTarget> getClazz() {
        return clazz;
    }
}
