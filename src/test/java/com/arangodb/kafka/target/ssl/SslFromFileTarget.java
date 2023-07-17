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

package com.arangodb.kafka.target.ssl;

import com.arangodb.kafka.config.ArangoSinkConfig;
import com.arangodb.kafka.target.converter.JsonTarget;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.Map;

public class SslFromFileTarget extends JsonTarget {
    private static final String LOCATION = "/tmp/test.truststore";

    public SslFromFileTarget(String name) {
        super(name);
    }

    @Override
    public void init() {
        try {
            Files.copy(
                    new File("src/test/resources/test.truststore").toPath(),
                    Paths.get(LOCATION),
                    StandardCopyOption.REPLACE_EXISTING);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        super.init();
    }

    @Override
    public Map<String, String> getConfig() {
        Map<String, String> cfg = super.getConfig();
        cfg.put(ArangoSinkConfig.CONNECTION_SSL_ENABLED, "true");
        cfg.put(ArangoSinkConfig.CONNECTION_SSL_TRUSTSTORE_LOCATION, LOCATION);
        cfg.put(ArangoSinkConfig.CONNECTION_SSL_TRUSTSTORE_PASSWORD, "12345678");
        cfg.put(ArangoSinkConfig.CONNECTION_SSL_HOSTNAME_VERIFICATION, "false");
        return cfg;
    }

}
