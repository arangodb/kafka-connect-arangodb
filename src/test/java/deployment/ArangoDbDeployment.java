package deployment;

import java.util.List;

public interface ArangoDbDeployment {

    static ArangoDbDeployment getInstance() {
        if (!Boolean.parseBoolean(System.getProperty("resilienceTests"))) {
            return PlainArangoDbDeployment.INSTANCE;
        } else {
            return ProxiedArangoDbDeployment.INSTANCE;
        }
    }

    String getEndpoints();

    default String getAdminEndpoints() {
        return getEndpoints();
    }

    default List<ProxiedEndpoint> getProxiedEndpoints() {
        throw new UnsupportedOperationException();
    }

}
