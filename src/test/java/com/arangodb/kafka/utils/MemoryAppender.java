package com.arangodb.kafka.utils;

import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;

import java.util.Collections;
import java.util.List;

public class MemoryAppender extends ListAppender<ILoggingEvent> {
    public void reset() {
        list.clear();
    }

    public List<ILoggingEvent> getLogs() {
        return Collections.unmodifiableList(list);
    }
}
