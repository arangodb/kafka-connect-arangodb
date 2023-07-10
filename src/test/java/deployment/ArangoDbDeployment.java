package deployment;

import com.arangodb.config.HostDescription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class ArangoDbDeployment {
    private static final Logger LOG;
    private final static String endpoints;
    private static final List<HostDescription> hosts;

    static {
        LOG = LoggerFactory.getLogger(ArangoDbDeployment.class);
        endpoints = System.getProperty("arango.endpoints", "172.28.0.1:8529");
        LOG.info("Using arango.endpoints: {}", endpoints);
        assert !endpoints.isEmpty();

        hosts = new ArrayList<>();
        for (String ep : endpoints.split(",")) {
            hosts.add(HostDescription.parse(ep));
        }
        assert hosts.size() > 0;
    }

    public static String getEndpoints() {
        return endpoints;
    }

    public static HostDescription getHost() {
        return hosts.get(0);
    }

}
