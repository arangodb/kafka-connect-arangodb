/*
 * Copyright 2023 ArangoDB GmbH, Cologne, Germany
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * Copyright holder is ArangoDB GmbH, Cologne, Germany
 */

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
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.sink.ErrantRecordReporter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Map;

public class ArangoSinkTask extends SinkTask {
    private static final Logger LOG = LoggerFactory.getLogger(ArangoSinkTask.class);
    private DocumentCreateOptions createOptions;
    private DocumentDeleteOptions deleteOptions;
    private boolean deleteEnabled;
    private KeyConverter keyConverter;
    private RecordConverter converter;
    private ArangoCollection col;
    private ErrantRecordReporter reporter;

    private int maxRetries;
    private int remainingRetries;

    @Override
    public String version() {
        return "1.0.0-SNAPSHOT";
    }

    @Override
    public void start(Map<String, String> props) {
        LOG.info("Starting ArangoSinkTask.");
        LOG.info("task config: {}", props);

        ArangoSinkConfig config = new ArangoSinkConfig(props);
        createOptions = config.getCreateOptions();
        deleteOptions = config.getDeleteOptions();
        deleteEnabled = config.isDeleteEnabled();
        keyConverter = new KeyConverter();
        converter = new RecordConverter(keyConverter);
        col = config.createCollection();
        reporter = context.errantRecordReporter();
        if (reporter == null) {
            LOG.warn("Errant record reporter not configured.");
        }
        maxRetries = config.getMaxRetries();
        remainingRetries = maxRetries;
        context.timeout(config.getRetryBackoffMs());

        config.logUnused();
    }

    @Override
    public void put(Collection<SinkRecord> records) {
        if (records.isEmpty()) {
            return;
        }

        LOG.trace("Handling {} record(s)", records.size());
        for (SinkRecord record : records) {
            try {
                handleRecord(record);
            } catch (RetriableException e) {
                throw e;
            } catch (Exception e) {
                if (reporter != null) {
                    LOG.debug("Reporting exception to DLQ:", e);
                    reporter.report(record, e);
                } else {
                    throw e;
                }
            }
        }
    }

    private void handleRecord(SinkRecord record) {
        LOG.trace("Handling record: {}-{}-{}", record.topic(), record.kafkaPartition(), record.kafkaOffset());
        if (record.key() != null && record.value() == null) {
            handleDelete(record);
        } else {
            handleInsert(record);
        }
        remainingRetries = maxRetries;
        LOG.trace("Completed handling record");
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
            if (e.getResponseCode() == 404) {
                if (e.getErrorNum() == 1202) {
                    // Response: 404, Error: 1202 - document not found
                    LOG.trace("Deleting document not found: {}", key);
                } else {
                    throw new ConnectException(e);
                }
            } else {
                handleRetriableException(new ConnectException(e));
            }
        } catch (Exception e) {
            handleRetriableException(new ConnectException(e));
        }
    }

    private void handleInsert(SinkRecord record) {
        ObjectNode doc = converter.convert(record);
        LOG.trace("Inserting document: {}", doc.get("_key"));
        try {
            col.insertDocument(doc, createOptions);
        } catch (ArangoDBException e) {
            Integer respCode = e.getResponseCode();
            if (respCode == 400 || respCode == 404 || respCode == 409 || respCode == 412) {
                throw new ConnectException(e);
            } else {
                handleRetriableException(new ConnectException(e));
            }
        } catch (Exception e) {
            handleRetriableException(new ConnectException(e));
        }
    }

    private void handleRetriableException(ConnectException e) {
        if (remainingRetries > 0) {
            remainingRetries--;
            throw new RetriableException(e);
        } else {
            remainingRetries = maxRetries;
            throw e;
        }
    }

    @Override
    public void stop() {
        LOG.info("Stopping ArangoSinkTask.");
        if (col != null) {
            col.db().arango().shutdown();
        }
    }
}
