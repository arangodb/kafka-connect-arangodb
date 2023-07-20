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
import com.arangodb.kafka.target.converter.ConvertTargets;
import com.arangodb.kafka.target.converter.JsonTarget;
import com.arangodb.kafka.target.converter.StringTarget;
import com.arangodb.kafka.utils.KafkaTest;

import java.util.Collections;

import static com.arangodb.kafka.utils.Utils.awaitCount;
import static com.arangodb.kafka.utils.Utils.map;
import static org.assertj.core.api.Assertions.assertThat;


class ConverterIT {

    @KafkaTest(group = ConvertTargets.class)
    void testConversionWithNoKey(ArangoCollection col, Producer producer) {
        assertThat(col.count().getCount()).isEqualTo(0L);
        producer.produce(null, map());
        awaitCount(col, 1);
        BaseDocument doc = col.db().query(
                "FOR d IN @@col RETURN d",
                BaseDocument.class,
                Collections.singletonMap("@col", col.name())
        ).next();
        assertThat(doc.getKey()).startsWith(producer.getTopicName());
    }

    @KafkaTest(group = ConvertTargets.class)
    void testConversionWithNullKeyField(ArangoCollection col, Producer producer) {
        assertThat(col.count().getCount()).isEqualTo(0L);
        producer.produce(null, map()
                .add("_key", null)
                .add("foo", null)
        );
        awaitCount(col, 1);

        Iterable<BaseDocument> docs = col.db().query(
                "FOR d IN @@col RETURN d",
                BaseDocument.class,
                Collections.singletonMap("@col", col.name())
        );
        assertThat(docs).allSatisfy(doc -> {
            assertThat(doc.getKey()).startsWith(producer.getTopicName());
            assertThat(doc.getProperties()).containsKey("foo");
            assertThat(doc.getAttribute("foo")).isNull();
        });
    }

    @KafkaTest(group = ConvertTargets.class)
    void testConversionWithKeyField(ArangoCollection col, Producer producer) {
        assertThat(col.count().getCount()).isEqualTo(0L);
        producer.produce(null, map().add("_key", "key"));
        awaitCount(col, 1);
        assertThat(col.documentExists("key")).isTrue();
    }

    @KafkaTest({JsonTarget.class, StringTarget.class})
    void testConversionWithNumericKeyField(ArangoCollection col, Producer producer) {
        assertThat(col.count().getCount()).isEqualTo(0L);
        producer.produce(null, map().add("_key", 1));
        awaitCount(col, 1);
        assertThat(col.documentExists("1")).isTrue();
    }

    @KafkaTest(group = ConvertTargets.class)
    void testConversionWithRecordId(ArangoCollection col, Producer producer) {
        assertThat(col.count().getCount()).isEqualTo(0L);
        producer.produce("id", map().add("foo", "bar"));
        awaitCount(col, 1);
        BaseDocument doc = col.getDocument("id", BaseDocument.class);
        assertThat(doc).isNotNull();
        assertThat(doc.getAttribute("foo")).isEqualTo("bar");
    }

    @KafkaTest(group = ConvertTargets.class)
    void testConversionWithNumericRecordId(ArangoCollection col, Producer producer) {
        assertThat(col.count().getCount()).isEqualTo(0L);
        producer.produce(1, map().add("foo", "bar"));
        awaitCount(col, 1);
        BaseDocument doc = col.getDocument("1", BaseDocument.class);
        assertThat(doc).isNotNull();
        assertThat(doc.getAttribute("foo")).isEqualTo("bar");
    }

    @KafkaTest(group = ConvertTargets.class)
    void testConversionNullValues(ArangoCollection col, Producer producer) {
        assertThat(col.count().getCount()).isEqualTo(0L);
        producer.produce(null, null);
        awaitCount(col, 1);
    }

}
