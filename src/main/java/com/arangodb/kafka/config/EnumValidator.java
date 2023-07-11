package com.arangodb.kafka.config;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;

import java.util.Arrays;

public class EnumValidator implements ConfigDef.Validator {
    private final Class<? extends Enum> enumClass;

    public EnumValidator(Class<? extends Enum> enumClass) {
        this.enumClass = enumClass;
    }

    @Override
    public void ensureValid(String name, Object value) {
        try {
            //noinspection unchecked
            Enum.valueOf(enumClass, (String) value);
        } catch (IllegalArgumentException e) {
            throw new ConfigException("Invalid value '" + value + "' for config key '" + name + "'" +
                    "; must be one of " + Arrays.toString(enumClass.getEnumConstants()));
        }
    }

    @Override
    public String toString() {
        return "One of " + Arrays.toString(enumClass.getEnumConstants());
    }
}
