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

import java.util.Collection;

public class ArangoWriter {
    private final static Logger LOG = LoggerFactory.getLogger(ArangoWriter.class);
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
    private final boolean tolerateTransientErrors;
    private int remainingRetries;

    public ArangoWriter(ArangoSinkConfig config, ArangoCollection col, ErrantRecordReporter reporter, SinkTaskContext context) {
        createOptions = config.getCreateOptions();
        deleteOptions = config.getDeleteOptions();
        deleteEnabled = config.isDeleteEnabled();
        retryBackoffMs = config.getRetryBackoffMs();
        maxRetries = config.getMaxRetries();
        remainingRetries = maxRetries;
        tolerateTransientErrors = config.tolerateTransientErrors();

        this.col = col;
        this.reporter = reporter;
        this.context = context;

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
                LOG.trace("Handling record: {}-{}-{}", record.topic(), record.kafkaPartition(), record.kafkaOffset());
                if (record.key() != null && record.value() == null) {
                    handleDelete(record);
                } else {
                    handleInsert(record);
                }
                remainingRetries = maxRetries;
                LOG.trace("Completed handling record");
            } catch (Exception e) {
                if (isFatalException(e)) {
                    reportException(record, e);
                } else {
                    handleTransientException(record, e);
                }
            }
        }
    }

    private void handleDelete(SinkRecord record) {
        if (!deleteEnabled) {
            throw new ConnectException("Deletes are not enabled. To enable set: "
                    + ArangoSinkConfig.DELETE_ENABLED + "=true");
        }

        String key = keyConverter.convert(record);
        try {
            LOG.trace("Deleting document: {}", key);
            col.deleteDocument(key, deleteOptions);
        } catch (ArangoDBException e) {
            if (e.getResponseCode() == 404 && e.getErrorNum() == 1202) {
                // Response: 404, Error: 1202 - document not found
                LOG.trace("Deleting document not found: {}", key);
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

    private boolean isFatalException(Exception ex) {
        if (ex instanceof DataException) {
            return true;
        }

        if (ex instanceof ArangoDBException) {
            ArangoDBException e = (ArangoDBException) ex;
            Integer responseCode = e.getResponseCode();
            if (responseCode == null || responseCode < 400 || responseCode >= 500) {
                return false;
            }

            Integer errorNum = e.getErrorNum();
            return errorNum != 18 &&        // lock timeout
                    errorNum != 1004 &&     // read only, i.e. not enough replicas for write-concern
                    errorNum != 1429 &&     // not enough replicas for write-concern
                    errorNum != 1200 &&     // write-write conflicts
                    errorNum != 21004 &&    // queue time violated
                    responseCode != 408 &&  // request timeout
                    responseCode != 420;    // too many requests
        }

        return false;
    }

    private void reportException(SinkRecord record, Exception e) {
        if (reporter != null) {
            LOG.debug("Reporting exception to DLQ:", e);
            reporter.report(record, e);
            remainingRetries = maxRetries;
        } else {
            throw new ConnectException(e);
        }
    }

    private void handleTransientException(SinkRecord record, Exception e) {
        if (remainingRetries > 0) {
            remainingRetries--;
            context.timeout(retryBackoffMs);
            throw new RetriableException(e);
        } else {
            if (tolerateTransientErrors) {
                reportException(record, e);
            } else {
                throw new ConnectException(e);
            }
        }
    }

}
