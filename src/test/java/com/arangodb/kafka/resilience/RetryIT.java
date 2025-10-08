package com.arangodb.kafka.resilience;

import ch.qos.logback.classic.Level;
import com.arangodb.ArangoCollection;
import com.arangodb.kafka.ArangoWriter;
import com.arangodb.kafka.config.ArangoSinkConfig;
import com.arangodb.kafka.target.Producer;
import com.arangodb.kafka.target.resilience.RetryTarget;
import com.arangodb.kafka.utils.KafkaTest;
import com.arangodb.kafka.utils.MemoryAppender;
import deployment.ArangoDbDeployment;
import deployment.ProxiedEndpoint;
import eu.rekawek.toxiproxy.model.Toxic;
import eu.rekawek.toxiproxy.model.ToxicDirection;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.junit.jupiter.api.parallel.ResourceLock;

import java.io.IOException;
import java.util.List;

import static com.arangodb.kafka.utils.Utils.*;
import static org.assertj.core.api.Assertions.assertThat;

@ResourceLock("resilienceTests") // avoid parallel execution
@EnabledIfSystemProperty(named = "resilienceTests", matches = "true")
class RetryIT {

    private final List<ProxiedEndpoint> endpoints = ArangoDbDeployment.getInstance().getProxiedEndpoints();

    @BeforeEach
    void init() throws IOException {
        for (ProxiedEndpoint e : endpoints) {
            e.getProxy().enable();
            for (Toxic t : e.getProxy().toxics().getAll()) {
                t.remove();
            }
        }
    }

    @KafkaTest(RetryTarget.class)
    void retryOnConnectionRefused(ArangoCollection col, Producer producer) throws Exception {
        producer.produce("key1", map());
        awaitCount(col, 1);
        assertThat(col.documentExists("key1")).isTrue();

        for (ProxiedEndpoint e : endpoints) {
            e.getProxy().disable();
        }

        MemoryAppender logs = interceptLogger(ArangoWriter.class);

        producer.produce("key2", map());
        Thread.sleep(1_000);

        assertThat(col.count().getCount()).isEqualTo(1L);

        assertThat(logs.getLogs().stream().filter(it -> Level.WARN.equals(it.getLevel()))).anySatisfy(it -> {
            assertThat(it.getMessage()).contains("transient exception");
            assertThat(it.getThrowableProxy().getMessage()).contains("Cannot contact any host!");
        });

        for (ProxiedEndpoint e : endpoints) {
            e.getProxy().enable();
        }

        awaitCount(col, 2);
        assertThat(col.documentExists("key2")).isTrue();
    }

    @KafkaTest(RetryTarget.class)
    void retryOnLatency(ArangoCollection col, Producer producer, ArangoSinkConfig cfg) throws Exception {
        producer.produce("key1", map());
        awaitCount(col, 1);
        assertThat(col.documentExists("key1")).isTrue();

        for (ProxiedEndpoint e : endpoints) {
            e.getProxy().toxics().latency("latency", ToxicDirection.UPSTREAM, 10_000);
        }

        MemoryAppender logs = interceptLogger(ArangoWriter.class);

        producer.produce("key2", map());
        Thread.sleep(1_000);

        assertThat(col.count().getCount()).isEqualTo(1L);

        assertThat(logs.getLogs().stream().filter(it -> Level.WARN.equals(it.getLevel()))).anySatisfy(it -> {
            assertThat(it.getMessage()).contains("transient exception");
            if (!ArangoSinkConfig.Protocol.HTTP2.equals(cfg.getConnectionProtocol())) {
                // https://github.com/vert-x3/vertx-web/issues/2296
                assertThat(it.getThrowableProxy().getMessage()).contains("timeout");
            }
        });

        for (ProxiedEndpoint e : endpoints) {
            for (Toxic toxic : e.getProxy().toxics().getAll()) {
                toxic.remove();
            }
        }

        awaitCount(col, 2);
        assertThat(col.documentExists("key2")).isTrue();
    }

}
