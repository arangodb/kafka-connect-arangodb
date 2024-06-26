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

package com.arangodb.kafka.target.protocol;

import com.arangodb.kafka.config.ArangoSinkConfig;
import com.arangodb.kafka.target.converter.JsonTarget;

import java.util.Map;

import static com.arangodb.kafka.utils.Utils.isLessThanVersion;

public class VstTarget extends JsonTarget {

    public VstTarget(String name) {
        super(name);
    }

    @Override
    public Map<String, String> getConfig() {
        Map<String, String> cfg = super.getConfig();
        cfg.put(ArangoSinkConfig.CONNECTION_PROTOCOL, ArangoSinkConfig.Protocol.VST.toString());
        cfg.put(ArangoSinkConfig.CONNECTION_CONTENT_TYPE, ArangoSinkConfig.ContentType.VPACK.toString());
        return cfg;
    }

    @Override
    protected boolean supportsVersion(String version) {
        return isLessThanVersion(version, 3, 12, 0);
    }
}
