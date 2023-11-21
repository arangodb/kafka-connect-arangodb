package com.arangodb.kafka;

import ch.qos.logback.classic.Level;
import com.arangodb.ArangoCollection;
import com.arangodb.ArangoDBException;
import com.arangodb.entity.DocumentCreateEntity;
import com.arangodb.entity.ErrorEntity;
import com.arangodb.entity.MultiDocumentEntity;
import com.arangodb.kafka.config.ArangoSinkConfig;
import com.arangodb.kafka.utils.MemoryAppender;
import com.arangodb.kafka.utils.MockTest;
import com.arangodb.kafka.utils.Utils;
import deployment.ArangoDbDeployment;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.sink.ErrantRecordReporter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.Mockito;

import java.lang.reflect.Field;
import java.util.Collections;
import java.util.Map;

import static com.arangodb.kafka.config.ArangoSinkConfig.*;
import static com.arangodb.kafka.utils.Utils.interceptLogger;
import static com.arangodb.kafka.utils.Utils.map;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@MockTest
class ErrorHandlingTest {
    private Utils.FluentMap<String, Object> config() {
        return map()
                .add(CONNECTION_ENDPOINTS, ArangoDbDeployment.getInstance().getEndpoints())
                .add(CONNECTION_COLLECTION, "RetryTest");
    }

    @Mock
    ArangoCollection col;

    @Mock
    ErrantRecordReporter reporter;

    @Mock
    SinkTaskContext context;

    private final ArangoDBException transientException = createException(403, 1004);
    private final ErrorEntity transientErrorEntity = createErrorEntity(403, 1004);
    private final ErrorEntity dataErrorEntity = createErrorEntity(400, 1221);

    @Test
    void dataErrorsInBatchResultToleranceNoneShouldThrow() {
        Map<String, Object> cfg = config().add(DATA_ERRORS_TOLERANCE, DataErrorsTolerance.NONE.toString());

        Mockito.when(context.errantRecordReporter()).thenReturn(reporter);

        ArangoWriter writer = new ArangoWriter(new ArangoSinkConfig(cfg), col, context);
        SinkRecord record = new SinkRecord("topic", 1, null, "key", null, map(), 0);

        MultiDocumentEntity<DocumentCreateEntity<Void>> mockRes = new MultiDocumentEntity<>();
        mockRes.setDocumentsAndErrors(Collections.singletonList(dataErrorEntity));
        mockRes.setErrors(Collections.singletonList(dataErrorEntity));
        mockRes.setDocuments(Collections.emptyList());
        Mockito.when(col.insertDocuments(anyIterable(), any())).thenReturn(mockRes);

        Throwable thrown = catchThrowable(() -> writer.put(Collections.singleton(record)));
        assertThat(thrown).isInstanceOf(DataException.class);
        assertThat(thrown.getCause()).isInstanceOf(ArangoDBException.class);

        verify(reporter, never()).report(any(), any());
    }

    @Test
    void dataErrorsToleranceAllWithNoDlqInBatchResultShouldIgnore() {
        Map<String, Object> cfg = config()
                .add(DATA_ERRORS_TOLERANCE, DataErrorsTolerance.ALL.toString())
                .add(DATA_ERRORS_LOG_ENABLE, "true");

        MemoryAppender logs = interceptLogger(ArangoWriter.class);
        Mockito.when(context.errantRecordReporter()).thenReturn(null);

        ArangoWriter writer = new ArangoWriter(new ArangoSinkConfig(cfg), col, context);
        SinkRecord record = new SinkRecord("topic", 1, null, "key", null, map(), 0);

        MultiDocumentEntity<DocumentCreateEntity<Void>> mockRes = new MultiDocumentEntity<>();
        mockRes.setDocumentsAndErrors(Collections.singletonList(dataErrorEntity));
        mockRes.setErrors(Collections.singletonList(dataErrorEntity));
        mockRes.setDocuments(Collections.emptyList());
        Mockito.when(col.insertDocuments(anyIterable(), any())).thenReturn(mockRes);

        writer.put(Collections.singleton(record));

        assertThat(logs.getLogs().stream()).anySatisfy(it -> {
            assertThat(it.getLevel()).isEqualTo(Level.WARN);
            assertThat(it.getFormattedMessage())
                    .contains("Got data exception while processing record")
                    .contains("key=key")
                    .contains("value={}");
        });
    }

    @Test
    void dataErrorsToleranceAllDlqInBatchResultShouldReport() {
        Map<String, Object> cfg = config().add(DATA_ERRORS_TOLERANCE, DataErrorsTolerance.ALL.toString());

        Mockito.when(context.errantRecordReporter()).thenReturn(reporter);
        ArangoWriter writer = new ArangoWriter(new ArangoSinkConfig(cfg), col, context);
        SinkRecord record = new SinkRecord("topic", 1, null, "key", null, map(), 0);

        MultiDocumentEntity<DocumentCreateEntity<Void>> mockRes = new MultiDocumentEntity<>();
        mockRes.setDocumentsAndErrors(Collections.singletonList(dataErrorEntity));
        mockRes.setErrors(Collections.singletonList(dataErrorEntity));
        mockRes.setDocuments(Collections.emptyList());
        Mockito.when(col.insertDocuments(anyIterable(), any())).thenReturn(mockRes);

        writer.put(Collections.singleton(record));

        verify(reporter, times(1)).report(refEq(record), argThat(e -> {
            assertThat(e).isInstanceOf(DataException.class);
            assertThat(e.getCause()).isInstanceOf(ArangoDBException.class);
            return true;
        }));
    }

    @Test
    void transientErrorsInBatchResultShouldRetryAndThenThrow() {
        Map<String, Object> cfg = config()
                .add(MAX_RETRIES, "1")
                .add(RETRY_BACKOFF_MS, "222");

        Mockito.when(context.errantRecordReporter()).thenReturn(reporter);

        ArangoWriter writer = new ArangoWriter(new ArangoSinkConfig(cfg), col, context);
        SinkRecord record = new SinkRecord("topic", 1, null, "key", null, map(), 0);

        MultiDocumentEntity<DocumentCreateEntity<Void>> mockRes = new MultiDocumentEntity<>();
        mockRes.setDocumentsAndErrors(Collections.singletonList(transientErrorEntity));
        mockRes.setErrors(Collections.singletonList(transientErrorEntity));
        mockRes.setDocuments(Collections.emptyList());
        Mockito.when(col.insertDocuments(anyIterable(), any())).thenReturn(mockRes);

        Throwable thrown = catchThrowable(() -> writer.put(Collections.singleton(record)));
        assertThat(thrown).isInstanceOf(RetriableException.class);
        assertThat(thrown.getCause()).isInstanceOf(TransientException.class);
        assertThat(thrown.getCause().getCause()).isInstanceOf(ArangoDBException.class);

        verify(context, times(1)).timeout(222);
        reset(context);

        Throwable thrown2 = catchThrowable(() -> writer.put(Collections.singleton(record)));
        assertThat(thrown2).isInstanceOf(TransientException.class);
        assertThat(thrown2.getCause()).isInstanceOf(ArangoDBException.class);

        verify(context, never()).timeout(anyLong());
        verify(reporter, never()).report(any(), any());
    }

    @Test
    void transientErrorsShouldRetryAndThenThrow() {
        Map<String, Object> cfg = config()
                .add(MAX_RETRIES, "1")
                .add(RETRY_BACKOFF_MS, "222");

        Mockito.when(context.errantRecordReporter()).thenReturn(reporter);

        ArangoWriter writer = new ArangoWriter(new ArangoSinkConfig(cfg), col, context);
        SinkRecord record = new SinkRecord("topic", 1, null, "key", null, map(), 0);
        Mockito.when(col.insertDocuments(anyIterable(), any())).thenThrow(transientException);

        Throwable thrown = catchThrowable(() -> writer.put(Collections.singleton(record)));
        assertThat(thrown).isInstanceOf(RetriableException.class);
        assertThat(thrown.getCause()).isInstanceOf(TransientException.class);
        assertThat(thrown.getCause().getCause()).isInstanceOf(ArangoDBException.class);

        verify(context, times(1)).timeout(222);
        reset(context);

        Throwable thrown2 = catchThrowable(() -> writer.put(Collections.singleton(record)));
        assertThat(thrown2).isInstanceOf(TransientException.class);
        assertThat(thrown2.getCause()).isInstanceOf(ArangoDBException.class);

        verify(context, never()).timeout(anyLong());
        verify(reporter, never()).report(any(), any());
    }

    @Test
    void transientErrorsInBatchResultWithNoRetriesShouldThrow() {
        Map<String, Object> cfg = config().add(MAX_RETRIES, "0");

        Mockito.when(context.errantRecordReporter()).thenReturn(reporter);

        ArangoWriter writer = new ArangoWriter(new ArangoSinkConfig(cfg), col, context);
        SinkRecord record = new SinkRecord("topic", 1, null, "key", null, map(), 0);

        MultiDocumentEntity<DocumentCreateEntity<Void>> mockRes = new MultiDocumentEntity<>();
        mockRes.setDocumentsAndErrors(Collections.singletonList(transientErrorEntity));
        mockRes.setErrors(Collections.singletonList(transientErrorEntity));
        mockRes.setDocuments(Collections.emptyList());
        Mockito.when(col.insertDocuments(anyIterable(), any())).thenReturn(mockRes);

        Throwable thrown2 = catchThrowable(() -> writer.put(Collections.singleton(record)));
        assertThat(thrown2).isInstanceOf(TransientException.class);
        assertThat(thrown2.getCause()).isInstanceOf(ArangoDBException.class);

        verify(context, never()).timeout(anyLong());
        verify(reporter, never()).report(any(), any());
    }

    @Test
    void transientErrorsWithNoRetriesShouldThrow() {
        Map<String, Object> cfg = config().add(MAX_RETRIES, "0");

        Mockito.when(context.errantRecordReporter()).thenReturn(reporter);

        ArangoWriter writer = new ArangoWriter(new ArangoSinkConfig(cfg), col, context);
        SinkRecord record = new SinkRecord("topic", 1, null, "key", null, map(), 0);
        Mockito.when(col.insertDocuments(anyIterable(), any())).thenThrow(transientException);

        Throwable thrown2 = catchThrowable(() -> writer.put(Collections.singleton(record)));
        assertThat(thrown2).isInstanceOf(TransientException.class);
        assertThat(thrown2.getCause()).isInstanceOf(ArangoDBException.class);

        verify(context, never()).timeout(anyLong());
        verify(reporter, never()).report(any(), any());
    }

    @Test
    void deleteDisabled() {
        Map<String, Object> cfg = config().add(MAX_RETRIES, "0");

        Mockito.when(context.errantRecordReporter()).thenReturn(reporter);

        ArangoWriter writer = new ArangoWriter(new ArangoSinkConfig(cfg), col, context);
        SinkRecord record = new SinkRecord("topic", 1, null, "key", null, null, 0);

        Throwable thrown = catchThrowable(() -> writer.put(Collections.singleton(record)));
        assertThat(thrown)
                .isInstanceOf(TransientException.class)
                .hasMessageContaining("Deletes are not enabled");

        verify(context, never()).timeout(anyLong());
        verify(reporter, never()).report(any(), any());
    }

    @Test
    void extraDataErrorsNumsInBatchResult() {
        Map<String, Object> cfg = config()
                .add(DATA_ERRORS_TOLERANCE, DataErrorsTolerance.NONE.toString())
                .add(EXTRA_DATA_ERRORS_NUMS, "1,2,1004");

        Mockito.when(context.errantRecordReporter()).thenReturn(reporter);

        ArangoWriter writer = new ArangoWriter(new ArangoSinkConfig(cfg), col, context);
        SinkRecord record = new SinkRecord("topic", 1, null, "key", null, map(), 0);
        MultiDocumentEntity<DocumentCreateEntity<Void>> mockRes = new MultiDocumentEntity<>();
        mockRes.setDocumentsAndErrors(Collections.singletonList(transientErrorEntity));
        mockRes.setErrors(Collections.singletonList(transientErrorEntity));
        mockRes.setDocuments(Collections.emptyList());
        Mockito.when(col.insertDocuments(anyIterable(), any())).thenReturn(mockRes);

        Throwable thrown = catchThrowable(() -> writer.put(Collections.singleton(record)));
        assertThat(thrown).isInstanceOf(DataException.class);
        assertThat(thrown.getCause()).isInstanceOf(ArangoDBException.class);

        verify(reporter, never()).report(any(), any());
    }

    private ArangoDBException createException(int code, int errNum) {
        return new ArangoDBException(createErrorEntity(code, errNum));
    }

    private ErrorEntity createErrorEntity(int code, int errNum) {
        ErrorEntity ee = new ErrorEntity();

        try {
            Field cf = ee.getClass().getDeclaredField("code");
            cf.setAccessible(true);
            cf.setInt(ee, code);

            Field ef = ee.getClass().getDeclaredField("errorNum");
            ef.setAccessible(true);
            ef.setInt(ee, errNum);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        return ee;
    }

}
