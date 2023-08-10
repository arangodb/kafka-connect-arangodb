package com.arangodb.kafka;

import org.apache.kafka.connect.errors.ConnectException;

public class TransientException extends ConnectException {
    public TransientException(Throwable cause) {
        super(cause);
    }
}
