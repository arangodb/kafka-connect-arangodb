package com.arangodb.kafka;

import com.arangodb.ArangoCollection;
import com.arangodb.ArangoDBException;
import com.arangodb.kafka.config.ArangoSinkConfig;
import com.arangodb.kafka.deployment.ArangoDbDeployment;
import com.arangodb.kafka.utils.Utils;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.sink.ErrantRecordReporter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Collections;
import java.util.Map;

import static com.arangodb.kafka.utils.Utils.map;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
public class RetryTest {
    private Utils.FluentMap<String, Object> config() {
        return map()
                .add("key.converter.schemas.enable", "false")
                .add("value.converter.schemas.enable", "false")
                .add(ArangoSinkConfig.CONNECTION_ENDPOINTS, ArangoDbDeployment.getEndpoints())
                .add(ArangoSinkConfig.CONNECTION_COLLECTION, "RetryTest");
    }

    @Mock
    ArangoCollection col;

    @Mock
    ErrantRecordReporter reporter;

    @Mock
    SinkTaskContext context;

    @Test
    void transientErrorsToleranceNoneShouldThrow() {
        Map<String, Object> cfg = config()
                .add(ArangoSinkConfig.MAX_RETRIES, "0")
                .add(ArangoSinkConfig.TRANSIENT_ERRORS_TOLERANCE, ArangoSinkConfig.TransientErrorsTolerance.NONE.toString());

        ArangoWriter writer = new ArangoWriter(new ArangoSinkConfig(cfg), col, reporter, context);
        SinkRecord record = new SinkRecord("topic", 1, null, "key", null, map(), 0);
        Mockito.when(col.insertDocument(any(), any())).thenThrow(new ArangoDBException("transient exception"));

        Throwable thrown = catchThrowable(() -> writer.put(Collections.singleton(record)));
        assertThat(thrown).isInstanceOf(ConnectException.class);
        assertThat(thrown.getCause()).isInstanceOf(ArangoDBException.class);
        assertThat(thrown.getCause()).hasMessage("transient exception");
    }

    @Test
    void transientErrorsToleranceNoneShouldRetry() {
        Map<String, Object> cfg = config()
                .add(ArangoSinkConfig.MAX_RETRIES, "3")
                .add(ArangoSinkConfig.RETRY_BACKOFF_MS, "222")
                .add(ArangoSinkConfig.TRANSIENT_ERRORS_TOLERANCE, ArangoSinkConfig.TransientErrorsTolerance.NONE.toString());

        ArangoWriter writer = new ArangoWriter(new ArangoSinkConfig(cfg), col, reporter, context);
        SinkRecord record = new SinkRecord("topic", 1, null, "key", null, map(), 0);
        Mockito.when(col.insertDocument(any(), any())).thenThrow(new ArangoDBException("transient exception"));

        Throwable thrown = catchThrowable(() -> writer.put(Collections.singleton(record)));
        assertThat(thrown).isInstanceOf(RetriableException.class);
        assertThat(thrown.getCause()).isInstanceOf(ArangoDBException.class);
        assertThat(thrown.getCause()).hasMessage("transient exception");

        verify(context, times(1)).timeout(222);
    }

    @Test
    void transientErrorsToleranceAllShouldReport() {
        Map<String, Object> cfg = config()
                .add(ArangoSinkConfig.MAX_RETRIES, "0")
                .add(ArangoSinkConfig.TRANSIENT_ERRORS_TOLERANCE, ArangoSinkConfig.TransientErrorsTolerance.ALL.toString());

        ArangoWriter writer = new ArangoWriter(new ArangoSinkConfig(cfg), col, reporter, context);
        SinkRecord record = new SinkRecord("topic", 1, null, "key", null, map(), 0);
        Mockito.when(col.insertDocument(any(), any())).thenThrow(new ArangoDBException("transient exception"));
        writer.put(Collections.singleton(record));

        verify(reporter, times(1)).report(refEq(record), argThat(e -> {
            assertThat(e).isInstanceOf(ArangoDBException.class);
            assertThat(e).hasMessage("transient exception");
            return true;
        }));
    }

    @Test
    void fatalErrorsShouldReport() {
        Map<String, Object> cfg = config();

        ArangoWriter writer = new ArangoWriter(new ArangoSinkConfig(cfg), col, reporter, context);
        SinkRecord record = new SinkRecord("topic", 1, null, "key", null, map(), 0);
        Mockito.when(col.insertDocument(any(), any())).thenThrow(new DataException("fatal exception"));
        writer.put(Collections.singleton(record));

        verify(reporter, times(1)).report(refEq(record), argThat(e -> {
            assertThat(e).isInstanceOf(DataException.class);
            assertThat(e).hasMessage("fatal exception");
            return true;
        }));
    }

}
