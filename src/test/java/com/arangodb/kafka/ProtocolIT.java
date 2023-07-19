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
import com.arangodb.kafka.target.Producer;
import com.arangodb.kafka.target.protocol.ProtocolTargets;
import com.arangodb.kafka.utils.KafkaTest;

import java.util.stream.IntStream;

import static com.arangodb.kafka.utils.Utils.*;
import static org.assertj.core.api.Assertions.assertThat;


class ProtocolIT {

    @KafkaTest(group = ProtocolTargets.class)
    void testWrite(ArangoCollection col, Producer producer) {
        assertThat(col.count().getCount()).isEqualTo(0L);
        producer.produce(IntStream.range(0, 10).mapToObj(i ->
                record("k-" + i, map().add("foo", "bar-" + i))));
        awaitCount(col, 10);
        BaseDocument doc = col.getDocument("k-0", BaseDocument.class);
        assertThat(doc.getAttribute("foo")).isEqualTo("bar-0");
    }

}
