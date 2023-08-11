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

package deployment;

import java.util.Iterator;
import java.util.ServiceLoader;

public abstract class KafkaConnectDeployment {
    private static KafkaConnectDeployment instance;

    public static synchronized KafkaConnectDeployment getInstance() {
        if (instance == null) {
            ServiceLoader<KafkaConnectDeployment> loader = ServiceLoader.load(KafkaConnectDeployment.class);
            Iterator<KafkaConnectDeployment> it = loader.iterator();
            instance = it.next();
            if (it.hasNext()) {
                throw new IllegalStateException("Multiple implementations found for: deployment.KafkaConnectDeployment");
            }
            instance.start();
        }
        return instance;
    }

    public static String getKafkaConnectHost() {
        return System.getProperty("kafka.connect.host");
    }

    public abstract String getBootstrapServers();

    public abstract KafkaConnectOperations client();

    public abstract String getSchemaRegistryUrlConnect();

    public abstract void start();
}
