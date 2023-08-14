package com.arangodb.kafka;

import com.arangodb.ArangoCollection;
import com.arangodb.ArangoDBException;
import com.arangodb.kafka.config.ArangoSinkConfig;
import com.arangodb.kafka.conversion.KeyConverter;
import com.arangodb.kafka.conversion.RecordConverter;
import com.arangodb.model.DocumentCreateOptions;
import com.arangodb.model.DocumentDeleteOptions;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.sink.ErrantRecordReporter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

public class ArangoWriter {
    private final static Logger LOG = LoggerFactory.getLogger(ArangoWriter.class);
    private final static Set<Integer> DATA_ERROR_NUMS = new HashSet<>(Arrays.asList(
            600,    // invalid JSON object
            1208,   // illegal name (document violating smart collection key)
            1210,   // unique constraint violated
            1216,   // document too large
            1221,   // illegal document key
            1222,   // unexpected document key
            1226,   // missing document key
            1233,   // edge attribute missing or invalid
            1466,   // must not specify _key for this collection (_key is required to contain the shard key of both vertex collections)
            1469,   // must not change the value of a shard key attribute (_to shard key x differs from the 'to' part in the _key y)
            1504,   // number out of range
            1505,   // invalid geo coordinate value
            1524,   // too much nesting or too many objects
            1542,   // invalid argument type in call to function
            1543,   // invalid regex value
            1561,   // invalid arithmetic value
            1562,   // division by zero
            1563,   // array expected
            1569,   // FAIL(%s) called
            1572,   // invalid date value
            1578,   // disallowed dynamic call
            1593,   // computed values expression evaluation produced a runtime error
            1594,   // computed values expression evaluation produced a runtime error
            1620,   // schema validation failed
            4001,   // smart graph attribute not given
            4003,   // in smart vertex collections _key must be a string and prefixed with the value of the smart graph attribute
            4010    // non-disjoint edge found
    ));

    private final ArangoCollection col;
    private final ErrantRecordReporter reporter;
    private final SinkTaskContext context;
    private final KeyConverter keyConverter;
    private final RecordConverter converter;
    private final DocumentCreateOptions createOptions;
    private final DocumentDeleteOptions deleteOptions;
    private final boolean deleteEnabled;
    private final int maxRetries;
    private final int retryBackoffMs;
    private final boolean tolerateDataErrors;
    private final boolean logDataErrors;
    private int remainingRetries;
    private final Set<Integer> extraDataErrorsNums;

    public ArangoWriter(ArangoSinkConfig config, ArangoCollection col, SinkTaskContext context) {
        createOptions = config.getCreateOptions();
        deleteOptions = config.getDeleteOptions();
        deleteEnabled = config.isDeleteEnabled();
        retryBackoffMs = config.getRetryBackoffMs();
        maxRetries = config.getMaxRetries();
        remainingRetries = maxRetries;
        tolerateDataErrors = config.getTolerateDataErrors();
        logDataErrors = config.getLogDataErrors();
        extraDataErrorsNums = config.getExtraDataErrorsNums();

        this.col = col;
        this.context = context;

        reporter = context.errantRecordReporter();
        if (reporter == null) {
            LOG.info("Errant record reporter not configured.");
        }

        keyConverter = new KeyConverter();
        converter = new RecordConverter(keyConverter);
    }

    public void put(Collection<SinkRecord> records) {
        if (records.isEmpty()) {
            return;
        }

        LOG.trace("Handling {} record(s)", records.size());
        for (SinkRecord record : records) {
            try {
                handleRecord(record);
            } catch (DataException e) {
                handleDataException(record, e);
            } catch (TransientException e) {
                handleTransientException(record, e);
            }
        }
    }

    private void handleRecord(SinkRecord record) {
        try {
            LOG.trace("Handling record: {}-{}-{}", record.topic(), record.kafkaPartition(), record.kafkaOffset());
            if (record.key() != null && record.value() == null) {
                handleDelete(record);
            } else {
                handleInsert(record);
            }
            LOG.trace("Completed handling record");
            remainingRetries = maxRetries;
        } catch (Exception e) {
            throw wrapException(e);
        }
    }

    private void handleDelete(SinkRecord record) {
        if (!deleteEnabled) {
            throw new ConnectException("Deletes are not enabled.");
        }

        String key = keyConverter.convert(record);
        try {
            LOG.trace("Deleting document: {}", key);
            col.deleteDocument(key, deleteOptions);
        } catch (ArangoDBException e) {
            if (e.getResponseCode() == 404 && e.getErrorNum() == 1202) {
                // Response: 404, Error: 1202 - document not found
                LOG.debug("Deleting document not found: {}", key);
            } else {
                throw e;
            }
        }
    }

    private void handleInsert(SinkRecord record) {
        ObjectNode doc = converter.convert(record);
        LOG.trace("Inserting document: {}", doc.get("_key"));
        col.insertDocument(doc, createOptions);
    }

    private ConnectException wrapException(Exception e) {
        if (e instanceof DataException) {
            return (DataException) e;
        }
        if (e instanceof ArangoDBException) {
            Integer errNum = ((ArangoDBException) e).getErrorNum();
            if (DATA_ERROR_NUMS.contains(errNum) || extraDataErrorsNums.contains(errNum)) {
                return new DataException(e);
            }
        }
        return new TransientException(e);
    }

    private void handleDataException(SinkRecord record, DataException e) {
        if (logDataErrors) {
            LOG.warn("Got data exception while processing record: {}", record, e);
        }
        if (tolerateDataErrors) {
            if (reporter != null) {
                LOG.debug("Reporting exception to DLQ:", e);
                reporter.report(record, e);
            } else {
                LOG.debug("Ignoring exception:", e);
            }
        } else {
            throw e;
        }
    }

    private void handleTransientException(SinkRecord record, TransientException e) {
        LOG.debug("Got transient exception while processing record: {}", record, e);
        if (remainingRetries > 0) {
            LOG.debug("remaining retries: {}", remainingRetries);
            remainingRetries--;
            context.timeout(retryBackoffMs);
            throw new RetriableException(e);
        } else {
            remainingRetries = maxRetries;
            throw e;
        }
    }

}
