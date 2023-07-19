package com.arangodb.kafka.config;

import org.apache.kafka.common.config.ConfigDef;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class EnumRecommender implements ConfigDef.Recommender {
    private final List<Object> validValues;

    public EnumRecommender(Class<? extends Enum> streamFromClass) {
        List<String> names = new ArrayList<>();
        for (Enum value : streamFromClass.getEnumConstants()) {
            names.add(value.toString());
        }
        this.validValues = Collections.unmodifiableList(names);
    }

    @Override
    public List<Object> validValues(String name, Map<String, Object> parsedConfig) {
        return validValues;
    }

    @Override
    public boolean visible(String name, Map<String, Object> parsedConfig) {
        return true;
    }
}
