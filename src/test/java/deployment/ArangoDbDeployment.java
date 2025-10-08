package deployment;

import java.util.List;

public interface ArangoDbDeployment {

    static ArangoDbDeployment getInstance() {
        String resilienceTests = System.getProperty("resilienceTests");
        if (resilienceTests != null &&
                (resilienceTests.isEmpty() || resilienceTests.equalsIgnoreCase("true"))) {
            return ProxiedArangoDbDeployment.INSTANCE;
        } else {
            return PlainArangoDbDeployment.INSTANCE;
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
