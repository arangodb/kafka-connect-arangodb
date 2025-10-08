package com.arangodb.kafka.resilience;

import com.arangodb.ArangoCollection;
import com.arangodb.kafka.config.ArangoSinkConfig;
import com.arangodb.kafka.target.Producer;
import com.arangodb.kafka.target.resilience.RetryTarget;
import com.arangodb.kafka.utils.KafkaTest;
import deployment.ArangoDbDeployment;
import deployment.ProxiedEndpoint;
import eu.rekawek.toxiproxy.Proxy;
import eu.rekawek.toxiproxy.model.Toxic;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.junit.jupiter.api.parallel.ResourceLock;

import java.io.IOException;
import java.util.List;

import static com.arangodb.kafka.utils.Utils.awaitCount;
import static com.arangodb.kafka.utils.Utils.map;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

@ResourceLock("resilienceTests") // avoid parallel execution
@EnabledIfSystemProperty(named = "resilienceTests", matches = "true")
class FailoverIT {

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
    void failover(ArangoCollection col, Producer producer, ArangoSinkConfig cfg) throws Exception {
        assumeTrue(cfg.getEndpoints().size() > 1, "cluster only");

        for (int i = 0; i < 10; i++) {
            producer.produce("key" + i, map());
        }
        awaitCount(col, 10);
        col.truncate();

        for (ProxiedEndpoint endpoint : endpoints) {
            Proxy proxy = endpoint.getProxy();
            proxy.disable();

            for (int i = 0; i < 10; i++) {
                producer.produce("key" + i, map());
            }
            awaitCount(col, 10);
            col.truncate();

            proxy.enable();
        }
    }

}
