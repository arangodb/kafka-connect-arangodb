package com.arangodb.kafka;

import com.arangodb.kafka.conversion.KeyConverter;
import com.arangodb.kafka.conversion.RecordConverter;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Base64;
import java.util.List;
import java.util.Map;

import static com.arangodb.kafka.utils.Utils.map;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

public class ConverterTest {

    private final RecordConverter converter = new RecordConverter(new KeyConverter());

    @Test
    void unsupportedValueFormat() {
        List<Object> values = Arrays.asList(
                true,
                11,
                22.33,
                "foo",
                Arrays.asList("a", "b", "c")
        );

        for (Object value : values) {
            SinkRecord record = new SinkRecord("topic", 1, null, "key", null, value, 0);

            Throwable thrown = catchThrowable(() -> converter.convert(record));
            assertThat(thrown)
                    .isInstanceOf(DataException.class)
                    .hasMessageContaining("Record value cannot be read as JSON object");
        }
    }

    @Test
    void invalidKeyTypes() {
        List<Object> keys = Arrays.asList(
                true,
                22.33,
                Arrays.asList("a", "b", "c"),
                map().add("foo", "bar")
        );

        for (Object key : keys) {
            SinkRecord record = new SinkRecord("topic", 1, null, key, null, map(), 0);

            Throwable thrown = catchThrowable(() -> converter.convert(record));
            assertThat(thrown)
                    .isInstanceOf(DataException.class)
                    .hasMessageContaining("Record key cannot be read as string");
        }
    }

    @Test
    void fieldsTypes() {
        Map<String, Object> value = map()
                .add("int", 11)
                .add("dec", 22.22)
                .add("bool", true)
                .add("str", "foo")
                .add("bytes", new byte[]{0x01, 0x02})
                .add("arr", Arrays.asList("a", 1, true))
                .add("null", null)
                .add("map", map().add("foo", "bar"));

        SinkRecord record = new SinkRecord("topic", 1, null, "key", null, value, 0);
        ObjectNode node = converter.convert(record);
        assertThat(node.get("int").isIntegralNumber()).isTrue();
        assertThat(node.get("int").asInt()).isEqualTo(11);
        assertThat(node.get("dec").isFloatingPointNumber()).isTrue();
        assertThat(node.get("dec").doubleValue()).isEqualTo(22.22);
        assertThat(node.get("bool").isBoolean()).isTrue();
        assertThat(node.get("bool").booleanValue()).isTrue();
        assertThat(node.get("str").isTextual()).isTrue();
        assertThat(node.get("str").textValue()).isEqualTo("foo");
        assertThat(node.get("bytes").isTextual()).isTrue();
        assertThat(node.get("bytes").textValue()).isEqualTo(Base64.getEncoder().encodeToString(new byte[]{0x01, 0x02}));
        assertThat(node.get("arr").isArray()).isTrue();
        assertThat(node.get("arr").get(0).textValue()).isEqualTo("a");
        assertThat(node.get("arr").get(1).intValue()).isEqualTo(1);
        assertThat(node.get("arr").get(2).booleanValue()).isTrue();
        assertThat(node.get("null").isNull()).isTrue();
        assertThat(node.get("map").isObject()).isTrue();
        assertThat(node.get("map").get("foo").textValue()).isEqualTo("bar");
    }

}
