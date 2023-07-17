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
import com.arangodb.entity.BaseDocument;
import com.arangodb.kafka.deployment.KafkaConnectOperations;
import com.arangodb.kafka.target.Connector;
import com.arangodb.kafka.target.Producer;
import com.arangodb.kafka.target.protocol.ProtocolTargets;
import com.arangodb.kafka.utils.KafkaTest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

import java.util.AbstractMap;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;


class ProtocolIT {

    @BeforeEach
    void setup(KafkaConnectOperations connectClient, Connector connector) {
        connectClient.createConnector(connector.getConfig());
    }

    @AfterEach
    void shutdown(KafkaConnectOperations connectClient, Connector connector) {
        connectClient.deleteConnector(connector.getName());
    }

    @KafkaTest(group = ProtocolTargets.class)
    void testWrite(ArangoCollection col, Producer producer) {
        assertThat(col.count().getCount()).isEqualTo(0L);

        producer.produce(IntStream.range(0, 10)
                .mapToObj(i -> {
                    Map<String, Object> data = new HashMap<>();
                    data.put("_key", "k-" + i);
                    data.put("foo", "bar-" + i);
                    return new AbstractMap.SimpleEntry<>(null, data);
                }));

        await().until(() -> col.count().getCount() >= 10L);

        BaseDocument doc0 = col.getDocument("k-0", BaseDocument.class);
        assertThat(doc0.getAttribute("foo")).isEqualTo("bar-0");
    }

}
