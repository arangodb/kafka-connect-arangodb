package com.arangodb.kafka.target;

public interface TargetHolder {
    Class<? extends TestTarget> getClazz();
}
