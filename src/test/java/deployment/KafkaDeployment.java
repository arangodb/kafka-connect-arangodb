package deployment;

interface KafkaDeployment {

    static KafkaDeployment getInstance() {
        return ExternalKafkaDeployment.INSTANCE;
    }

    static String getKafkaBootstrapServers() {
        return System.getProperty("kafka.bootstrap.servers", "172.28.11.1:9092");
    }

    void start();

    void stop();

    String getBootstrapServers();
}
