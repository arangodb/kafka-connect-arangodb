public interface KafkaConnectDeployment {

    static KafkaConnectDeployment getInstance() {
        String kafkaConnectHost = getKafkaConnectHost();
        if (kafkaConnectHost != null && !kafkaConnectHost.isEmpty()) {
            return ClusterKafkaConnectDeployment.INSTANCE;
        } else {
            return StandaloneKafkaConnectDeployment.INSTANCE;
        }
    }

    static String getKafkaConnectHost() {
        return System.getProperty("kafka.connect.host");
    }

    void start();

    void stop();

    String getBootstrapServers();

    KafkaConnectOperations client();

}
