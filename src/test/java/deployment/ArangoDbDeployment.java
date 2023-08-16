package deployment;

import com.arangodb.config.HostDescription;

import java.util.List;

public interface ArangoDbDeployment {

    static ArangoDbDeployment getInstance() {
        if (System.getProperty("resilienceTests") == null) {
            return PlainArangoDbDeployment.INSTANCE;
        } else {
            return ProxiedArangoDbDeployment.INSTANCE;
        }
    }

    String getEndpoints();

    default List<ProxiedEndpoint> getProxiedEndpoints() {
        throw new UnsupportedOperationException();
    }

}
