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
import com.arangodb.kafka.config.ArangoSinkConfig;
import com.arangodb.kafka.conversion.RecordConverter;
import com.arangodb.model.DocumentCreateOptions;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Map;

public class ArangoSinkTask extends SinkTask {
    private static final Logger LOG = LoggerFactory.getLogger(ArangoSinkTask.class);
    private DocumentCreateOptions documentCreateOptions;
    private RecordConverter converter;
    private ArangoCollection col;

    @Override
    public String version() {
        return "1.0.0-SNAPSHOT";
    }

    @Override
    public void start(Map<String, String> props) {
        LOG.info("Starting ArangoSinkTask.");
        LOG.info("task config: {}", props);

        ArangoSinkConfig config = new ArangoSinkConfig(props);
        documentCreateOptions = config.createInsertOptions();
        converter = new RecordConverter();
        col = config.createCollection();
    }

    @Override
    public void put(Collection<SinkRecord> records) {
        if (records.isEmpty()) {
            return;
        }

        LOG.info("writing {} record(s)", records.size());
        for (SinkRecord record : records) {
            LOG.info("rcv msg: {}-{}-{}", record.topic(), record.kafkaPartition(), record.kafkaOffset());
            col.insertDocument(converter.convert(record), documentCreateOptions);
        }
        LOG.info(col.db().getVersion().getVersion());
    }

    @Override
    public void stop() {
        LOG.info("Stopping ArangoSinkTask.");
        if (col != null) {
            col.db().arango().shutdown();
        }
    }
}
