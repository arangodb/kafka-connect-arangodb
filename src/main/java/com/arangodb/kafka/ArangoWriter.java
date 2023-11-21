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
import com.arangodb.entity.ErrorEntity;
import com.arangodb.kafka.config.ArangoSinkConfig;
import com.arangodb.kafka.conversion.KeyConverter;
import com.arangodb.kafka.conversion.RecordConverter;
import com.arangodb.model.DocumentCreateOptions;
import com.arangodb.model.DocumentDeleteOptions;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.sink.ErrantRecordReporter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class ArangoWriter {
    private enum Type {INSERT, DELETE}

    private final static Logger LOG = LoggerFactory.getLogger(ArangoWriter.class);
    private final static Set<Integer> DATA_ERROR_NUMS = new HashSet<>(Arrays.asList(
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
    private final int batchSize;
    private final boolean deleteEnabled;
    private final int maxRetries;
    private final int retryBackoffMs;
    private final boolean tolerateDataErrors;
    private final boolean logDataErrors;
    private final Set<Integer> extraDataErrorsNums;
    private int remainingRetries;
    private int currentOffset;
    private SinkRecord errorRecord;

    public ArangoWriter(ArangoSinkConfig config, ArangoCollection col, SinkTaskContext context) {
        createOptions = config.getCreateOptions();
        deleteOptions = config.getDeleteOptions();
        batchSize = config.getBatchSize();
        deleteEnabled = config.isDeleteEnabled();
        maxRetries = config.getMaxRetries();
        retryBackoffMs = config.getRetryBackoffMs();
        tolerateDataErrors = config.getTolerateDataErrors();
        logDataErrors = config.getLogDataErrors();
        extraDataErrorsNums = config.getExtraDataErrorsNums();
        remainingRetries = maxRetries;
        currentOffset = 0;
        errorRecord = null;

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
        ArrayList<SinkRecord> recordsList = new ArrayList<>(records);
        while (currentOffset < records.size()) {
            errorRecord = null;
            List<SinkRecord> currentBatch = extractBatch(recordsList);
            if (currentBatch.isEmpty()) {
                break;
            }

            LOG.trace("Handling batch of {} record(s)", currentBatch.size());
            try {
                handleBatch(currentBatch);
            } catch (DataException e) {
                handleDataException(e);
            } catch (TransientException e) {
                handleTransientException(e);
            }
            remainingRetries = maxRetries;
            currentOffset += currentBatch.size();
        }
        remainingRetries = maxRetries;
        currentOffset = 0;
    }

    private List<SinkRecord> extractBatch(List<SinkRecord> records) {
        List<SinkRecord> offsetRecords = records.subList(currentOffset, records.size());
        if (offsetRecords.isEmpty()) {
            return offsetRecords;
        }
        Type batchType = getType(offsetRecords.get(0));
        int toIndex = IntStream.range(0, offsetRecords.size())
                .filter(i -> !batchType.equals(getType(offsetRecords.get(i))))
                .findFirst()
                .orElse(offsetRecords.size());
        return offsetRecords.subList(0, Math.min(toIndex, batchSize));
    }

    private Type getType(SinkRecord record) {
        if (record.key() != null && record.value() == null) {
            return Type.DELETE;
        } else {
            return Type.INSERT;
        }
    }

    private void handleBatch(List<SinkRecord> batch) {
        if (LOG.isTraceEnabled()) {
            LOG.trace("Handling batch of {} records:", batch.size());
            for (SinkRecord record : batch) {
                LOG.trace("{}-{}-{}", record.topic(), record.kafkaPartition(), record.kafkaOffset());
            }
        }

        try {
            switch (getType(batch.get(0))) {
                case DELETE:
                    handleBatchDelete(batch);
                    break;
                case INSERT:
                    handleBatchInsert(batch);
                    break;
                default:
                    throw new IllegalStateException();
            }
            LOG.trace("Completed handling batch");
        } catch (Exception e) {
            throw wrapException(e);
        }
    }

    private void handleBatchDelete(List<SinkRecord> batch) {
        if (!deleteEnabled) {
            throw new ConnectException("Deletes are not enabled.");
        }

        List<String> keys = batch.stream()
                .map(keyConverter::convert)
                .collect(Collectors.toList());

        LOG.trace("Deleting documents: {}", keys);

        List<Object> docsAndErrs = col.deleteDocuments(keys, deleteOptions).getDocumentsAndErrors();
        checkResultSize(batch, docsAndErrs);

        List<SinkRecord> batchWithoutNotFound = new ArrayList<>();
        List<Object> docsAndErrsWithoutNotFound = new ArrayList<>();

        // remove docs not found, allow idempotent deletes
        for (int i = 0; i < docsAndErrs.size(); i++) {
            Object res = docsAndErrs.get(i);
            if (res instanceof ErrorEntity) {
                ErrorEntity e = (ErrorEntity) res;
                if (e.getErrorNum() == 1202) {
                    // Error: 1202 - document not found
                    LOG.debug("Deleting document not found: {}", keys.get(i));
                } else {
                    batchWithoutNotFound.add(batch.get(i));
                    docsAndErrsWithoutNotFound.add(docsAndErrs.get(i));
                }
            } else {
                batchWithoutNotFound.add(batch.get(i));
                docsAndErrsWithoutNotFound.add(docsAndErrs.get(i));
            }
        }

        checkTransientErrors(batchWithoutNotFound, docsAndErrsWithoutNotFound);
        checkDataErrors(batchWithoutNotFound, docsAndErrsWithoutNotFound);
    }

    private void handleBatchInsert(List<SinkRecord> batch) {
        List<ObjectNode> docs = batch.stream()
                .map(converter::convert)
                .collect(Collectors.toList());

        if (LOG.isTraceEnabled()) {
            List<JsonNode> keys = docs.stream()
                    .map(it -> it.get("_key"))
                    .collect(Collectors.toList());
            LOG.trace("Inserting documents: {}", keys);
        }

        List<Object> docsAndErrs = col.insertDocuments(docs, createOptions).getDocumentsAndErrors();
        checkResultSize(batch, docsAndErrs);
        checkTransientErrors(batch, docsAndErrs);
        checkDataErrors(batch, docsAndErrs);
    }

    private void checkTransientErrors(List<SinkRecord> batch, List<Object> docsAndErrs) {
        for (int i = 0; i < docsAndErrs.size(); i++) {
            Object res = docsAndErrs.get(i);
            if (res instanceof ErrorEntity) {
                ErrorEntity e = (ErrorEntity) res;
                if (!isDataError(e.getErrorNum())) {
                    errorRecord = batch.get(i);
                    throw new TransientException(new ArangoDBException(e));
                }
            }
        }
    }

    private void checkDataErrors(List<SinkRecord> batch, List<Object> docsAndErrs) {
        for (int i = 0; i < docsAndErrs.size(); i++) {
            Object res = docsAndErrs.get(i);
            if (res instanceof ErrorEntity) {
                ErrorEntity e = (ErrorEntity) res;
                if (isDataError(e.getErrorNum())) {
                    errorRecord = batch.get(i);
                    handleDataException(new DataException(new ArangoDBException(e)));
                }
            }
        }
    }

    private void checkResultSize(List<SinkRecord> batch, List<Object> docsAndErrs) {
        if (batch.size() != docsAndErrs.size()) {
            throw new ConnectException("Response length [" + docsAndErrs.size() + "] " +
                    "does not match batch length [" + batch.size() + "].");
        }
    }

    private ConnectException wrapException(Exception e) {
        if (e instanceof DataException) {
            return (DataException) e;
        }
        if (e instanceof ArangoDBException) {
            Integer errNum = ((ArangoDBException) e).getErrorNum();
            if (isDataError(errNum)) {
                return new DataException(e);
            }
        }
        if (e instanceof TransientException) {
            return (TransientException) e;
        }
        return new TransientException(e);
    }

    private boolean isDataError(Integer errNum) {
        return DATA_ERROR_NUMS.contains(errNum) || extraDataErrorsNums.contains(errNum);
    }

    private void handleDataException(DataException e) {
        if (errorRecord == null) {
            // data errors should only happen in batch results, for a specific document and not for the entire batch
            throw new ConnectException("Got data exception in batch write!");
        }

        if (logDataErrors) {
            LOG.warn("Got data exception while processing record: {}", errorRecord, e);
        } else {
            LOG.debug("Got data exception while processing record: {}", errorRecord, e);
        }

        if (tolerateDataErrors) {
            if (reporter != null) {
                LOG.debug("Reporting exception to DLQ:", e);
                reporter.report(errorRecord, e);
            } else {
                LOG.debug("Ignoring exception:", e);
            }
        } else {
            throw e;
        }
    }

    private void handleTransientException(TransientException e) {
        LOG.warn("Got transient exception: ", e);
        if (errorRecord != null) {
            LOG.debug("Got transient exception while processing record: {}", errorRecord, e);
        }
        if (remainingRetries > 0) {
            LOG.info("remaining retries: {}", remainingRetries);
            remainingRetries--;
            context.timeout(retryBackoffMs);
            throw new RetriableException(e);
        } else {
            remainingRetries = maxRetries;
            throw e;
        }
    }

}
