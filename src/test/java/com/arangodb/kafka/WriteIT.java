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
import com.arangodb.kafka.target.Producer;
import com.arangodb.kafka.target.write.DeleteTarget;
import com.arangodb.kafka.utils.KafkaTest;

import static com.arangodb.kafka.utils.Utils.*;
import static org.assertj.core.api.Assertions.assertThat;


class WriteIT {

    @KafkaTest(DeleteTarget.class)
    void insertMany(ArangoCollection col, Producer producer) {
        producer.produce("key1", map());
        producer.produce("key2", map());
        producer.produce("key3", map());
        producer.produce("key4", map());
        producer.produce("key5", map());
        producer.produce("flush", map());

        awaitKey(col, "flush");
        assertThat(col.documentExists("key1")).isTrue();
        assertThat(col.documentExists("key2")).isTrue();
        assertThat(col.documentExists("key3")).isTrue();
        assertThat(col.documentExists("key4")).isTrue();
        assertThat(col.documentExists("key5")).isTrue();
    }

    @KafkaTest(DeleteTarget.class)
    void delete(ArangoCollection col, Producer producer) {
        producer.produce("key", map().add("value", "foo"));
        awaitCount(col, 1);
        assertThat(col.documentExists("key")).isTrue();

        // delete
        producer.produce("key", null);
        awaitCount(col, eq(0));

        // test whether delete is idempotent
        producer.produce("key", null);

        producer.produce("flush", map());
        awaitCount(col, 1);
        assertThat(col.documentExists("flush")).isTrue();
    }

    @KafkaTest(DeleteTarget.class)
    void deleteWithDuplicatesKeys(ArangoCollection col, Producer producer) {
        producer.produce("key", map().add("value", "foo"));
        awaitCount(col, 1);
        assertThat(col.documentExists("key")).isTrue();

        // delete
        producer.produce("key", null);
        producer.produce("key", null);
        producer.produce("key", null);
        producer.produce("key", null);
        producer.produce("key", null);
        awaitCount(col, eq(0));

        producer.produce("flush", map());
        awaitCount(col, 1);
        assertThat(col.documentExists("flush")).isTrue();
    }

    @KafkaTest(DeleteTarget.class)
    void writeAndDelete(ArangoCollection col, Producer producer) {
        producer.produce("key1", map());
        producer.produce("key2", map());
        producer.produce("key3", map());
        producer.produce("key4", map());
        producer.produce("key5", map());
        producer.produce("key1", null);
        producer.produce("key2", null);
        producer.produce("key3", null);
        producer.produce("key4", null);
        producer.produce("flush", map());

        awaitKey(col, "flush");
        assertThat(col.documentExists("key1")).isFalse();
        assertThat(col.documentExists("key2")).isFalse();
        assertThat(col.documentExists("key3")).isFalse();
        assertThat(col.documentExists("key4")).isFalse();
        assertThat(col.documentExists("key5")).isTrue();
    }

}
