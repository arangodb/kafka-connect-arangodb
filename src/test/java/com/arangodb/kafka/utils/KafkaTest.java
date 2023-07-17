package com.arangodb.kafka.utils;

import com.arangodb.kafka.target.TargetHolder;
import com.arangodb.kafka.target.TestTarget;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.ExtendWith;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
@TestTemplate
@ExtendWith(TargetProvider.class)
@Timeout(30)
public @interface KafkaTest {
    Class<? extends TestTarget>[] value() default {};

    Class<? extends TargetHolder> group() default TargetHolder.class;
}
