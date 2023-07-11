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

package com.arangodb.kafka.config;

import com.arangodb.config.HostDescription;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.types.Password;

import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;

public class ArangoSinkConfig extends AbstractConfig {
    public enum Protocol {
        VST,
        HTTP11,
        HTTP2
    }

    public enum ContentType {
        JSON,
        VPACK
    }

    //region Connection
    private static final String CONNECTION_GROUP = "Connection";
    public static final String CONNECTION_PREFIX = "connection.";

    public static final String CONNECTION_ENDPOINTS = CONNECTION_PREFIX + "endpoints";
    private static final String CONNECTION_ENDPOINTS_DOC =
            "Database connection endpoints as comma separated list of `host:port` entries.\n"
                    + "For example: `coordinator1:8529,coordinator2:8529`";
    private static final String CONNECTION_ENDPOINTS_DISPLAY = "Endpoints";

    public static final String CONNECTION_USER = CONNECTION_PREFIX + "user";
    public static final String CONNECTION_USER_DEFAULT = "root";
    private static final String CONNECTION_USER_DOC = "Database connection user.";
    private static final String CONNECTION_USER_DISPLAY = "User";

    public static final String CONNECTION_PASSWORD = CONNECTION_PREFIX + "password";
    private static final String CONNECTION_PASSWORD_DOC = "Database connection password.";
    private static final String CONNECTION_PASSWORD_DISPLAY = "Password";

    public static final String CONNECTION_DATABASE = CONNECTION_PREFIX + "database";
    public static final String CONNECTION_DATABASE_DEFAULT = "_system";
    private static final String CONNECTION_DATABASE_DOC = "Target database name.";
    private static final String CONNECTION_DATABASE_DISPLAY = "Database";

    public static final String CONNECTION_COLLECTION = CONNECTION_PREFIX + "collection";
    private static final String CONNECTION_COLLECTION_DOC = "Target collection name.";
    private static final String CONNECTION_COLLECTION_DISPLAY = "Collection";

    public static final String CONNECTION_PROTOCOL = CONNECTION_PREFIX + "protocol";
    public static final String CONNECTION_PROTOCOL_DEFAULT = Protocol.HTTP2.toString();
    private static final String CONNECTION_PROTOCOL_DOC = "Communication protocol.";
    private static final String CONNECTION_PROTOCOL_DISPLAY = "Protocol";

    public static final String CONNECTION_CONTENT_TYPE = CONNECTION_PREFIX + "content-type";
    public static final String CONNECTION_CONTENT_TYPE_DEFAULT = ContentType.JSON.toString();
    private static final String CONNECTION_CONTENT_TYPE_DOC = "Communication content type.";
    private static final String CONNECTION_CONTENT_TYPE_DISPLAY = "Content Type";
    //endregion


    public static final ConfigDef CONFIG_DEF = new ConfigDef()

            // Connection
            .define(
                    CONNECTION_ENDPOINTS,
                    ConfigDef.Type.LIST,
                    ConfigDef.NO_DEFAULT_VALUE,
                    ConfigDef.Importance.HIGH,
                    CONNECTION_ENDPOINTS_DOC,
                    CONNECTION_GROUP,
                    1,
                    ConfigDef.Width.LONG,
                    CONNECTION_ENDPOINTS_DISPLAY
            )
            .define(
                    CONNECTION_USER,
                    ConfigDef.Type.STRING,
                    CONNECTION_USER_DEFAULT,
                    ConfigDef.Importance.HIGH,
                    CONNECTION_USER_DOC,
                    CONNECTION_GROUP,
                    2,
                    ConfigDef.Width.MEDIUM,
                    CONNECTION_USER_DISPLAY
            )
            .define(
                    CONNECTION_PASSWORD,
                    ConfigDef.Type.PASSWORD,
                    null,
                    ConfigDef.Importance.HIGH,
                    CONNECTION_PASSWORD_DOC,
                    CONNECTION_GROUP,
                    3,
                    ConfigDef.Width.MEDIUM,
                    CONNECTION_PASSWORD_DISPLAY
            )
            .define(
                    CONNECTION_DATABASE,
                    ConfigDef.Type.STRING,
                    CONNECTION_DATABASE_DEFAULT,
                    ConfigDef.Importance.HIGH,
                    CONNECTION_DATABASE_DOC,
                    CONNECTION_GROUP,
                    4,
                    ConfigDef.Width.MEDIUM,
                    CONNECTION_DATABASE_DISPLAY
            )
            .define(
                    CONNECTION_COLLECTION,
                    ConfigDef.Type.STRING,
                    ConfigDef.NO_DEFAULT_VALUE,
                    ConfigDef.Importance.HIGH,
                    CONNECTION_COLLECTION_DOC,
                    CONNECTION_GROUP,
                    5,
                    ConfigDef.Width.MEDIUM,
                    CONNECTION_COLLECTION_DISPLAY
            )
            .define(
                    CONNECTION_PROTOCOL,
                    ConfigDef.Type.STRING,
                    CONNECTION_PROTOCOL_DEFAULT,
                    new EnumValidator(Protocol.class),
                    ConfigDef.Importance.MEDIUM,
                    CONNECTION_PROTOCOL_DOC,
                    CONNECTION_GROUP,
                    6,
                    ConfigDef.Width.MEDIUM,
                    CONNECTION_PROTOCOL_DISPLAY,
                    new EnumRecommender(Protocol.class)
            )
            .define(
                    CONNECTION_CONTENT_TYPE,
                    ConfigDef.Type.STRING,
                    CONNECTION_CONTENT_TYPE_DEFAULT,
                    new EnumValidator(ContentType.class),
                    ConfigDef.Importance.LOW,
                    CONNECTION_CONTENT_TYPE_DOC,
                    CONNECTION_GROUP,
                    7,
                    ConfigDef.Width.MEDIUM,
                    CONNECTION_CONTENT_TYPE_DISPLAY,
                    new EnumRecommender(ContentType.class)
            )


//            .define(
//                    CONNECTION_ATTEMPTS,
//                    ConfigDef.Type.INT,
//                    CONNECTION_ATTEMPTS_DEFAULT,
//                    ConfigDef.Range.atLeast(1),
//                    ConfigDef.Importance.LOW,
//                    CONNECTION_ATTEMPTS_DOC,
//                    CONNECTION_GROUP,
//                    5,
//                    ConfigDef.Width.SHORT,
//                    CONNECTION_ATTEMPTS_DISPLAY
//            ).define(
//                    CONNECTION_BACKOFF,
//                    ConfigDef.Type.LONG,
//                    CONNECTION_BACKOFF_DEFAULT,
//                    ConfigDef.Importance.LOW,
//                    CONNECTION_BACKOFF_DOC,
//                    CONNECTION_GROUP,
//                    6,
//                    ConfigDef.Width.SHORT,
//                    CONNECTION_BACKOFF_DISPLAY
//            )


//            // Writes
//            .define(
//                    INSERT_MODE,
//                    ConfigDef.Type.STRING,
//                    INSERT_MODE_DEFAULT,
//                    EnumValidator.in(InsertMode.values()),
//                    ConfigDef.Importance.HIGH,
//                    INSERT_MODE_DOC,
//                    WRITES_GROUP,
//                    1,
//                    ConfigDef.Width.MEDIUM,
//                    INSERT_MODE_DISPLAY
//            )
//            .define(
//                    BATCH_SIZE,
//                    ConfigDef.Type.INT,
//                    BATCH_SIZE_DEFAULT,
//                    NON_NEGATIVE_INT_VALIDATOR,
//                    ConfigDef.Importance.MEDIUM,
//                    BATCH_SIZE_DOC, WRITES_GROUP,
//                    2,
//                    ConfigDef.Width.SHORT,
//                    BATCH_SIZE_DISPLAY
//            )
//            .define(
//                    DELETE_ENABLED,
//                    ConfigDef.Type.BOOLEAN,
//                    DELETE_ENABLED_DEFAULT,
//                    ConfigDef.Importance.MEDIUM,
//                    DELETE_ENABLED_DOC, WRITES_GROUP,
//                    3,
//                    ConfigDef.Width.SHORT,
//                    DELETE_ENABLED_DISPLAY,
//                    DeleteEnabledRecommender.INSTANCE
//            )
//            .define(
//                    TABLE_TYPES_CONFIG,
//                    ConfigDef.Type.LIST,
//                    TABLE_TYPES_DEFAULT,
//                    TABLE_TYPES_RECOMMENDER,
//                    ConfigDef.Importance.LOW,
//                    TABLE_TYPES_DOC,
//                    WRITES_GROUP,
//                    4,
//                    ConfigDef.Width.MEDIUM,
//                    TABLE_TYPES_DISPLAY
//            )


//            // Data Mapping
//            .define(
//                    TABLE_NAME_FORMAT,
//                    ConfigDef.Type.STRING,
//                    TABLE_NAME_FORMAT_DEFAULT,
//                    new ConfigDef.NonEmptyString(),
//                    ConfigDef.Importance.MEDIUM,
//                    TABLE_NAME_FORMAT_DOC,
//                    DATAMAPPING_GROUP,
//                    1,
//                    ConfigDef.Width.LONG,
//                    TABLE_NAME_FORMAT_DISPLAY
//            )
//            .define(
//                    PK_MODE,
//                    ConfigDef.Type.STRING,
//                    PK_MODE_DEFAULT,
//                    EnumValidator.in(PrimaryKeyMode.values()),
//                    ConfigDef.Importance.HIGH,
//                    PK_MODE_DOC,
//                    DATAMAPPING_GROUP,
//                    2,
//                    ConfigDef.Width.MEDIUM,
//                    PK_MODE_DISPLAY,
//                    PrimaryKeyModeRecommender.INSTANCE
//            )
//            .define(
//                    PK_FIELDS,
//                    ConfigDef.Type.LIST,
//                    PK_FIELDS_DEFAULT,
//                    ConfigDef.Importance.MEDIUM,
//                    PK_FIELDS_DOC,
//                    DATAMAPPING_GROUP,
//                    3,
//                    ConfigDef.Width.LONG, PK_FIELDS_DISPLAY
//            )
//            .define(
//                    FIELDS_WHITELIST,
//                    ConfigDef.Type.LIST,
//                    FIELDS_WHITELIST_DEFAULT,
//                    ConfigDef.Importance.MEDIUM,
//                    FIELDS_WHITELIST_DOC,
//                    DATAMAPPING_GROUP,
//                    4,
//                    ConfigDef.Width.LONG,
//                    FIELDS_WHITELIST_DISPLAY
//            ).define(
//                    DB_TIMEZONE_CONFIG,
//                    ConfigDef.Type.STRING,
//                    DB_TIMEZONE_DEFAULT,
//                    TimeZoneValidator.INSTANCE,
//                    ConfigDef.Importance.MEDIUM,
//                    DB_TIMEZONE_CONFIG_DOC,
//                    DATAMAPPING_GROUP,
//                    5,
//                    ConfigDef.Width.MEDIUM,
//                    DB_TIMEZONE_CONFIG_DISPLAY
//            )


//            // Retries
//            .define(
//                    MAX_RETRIES,
//                    ConfigDef.Type.INT,
//                    MAX_RETRIES_DEFAULT,
//                    NON_NEGATIVE_INT_VALIDATOR,
//                    ConfigDef.Importance.MEDIUM,
//                    MAX_RETRIES_DOC,
//                    RETRIES_GROUP,
//                    1,
//                    ConfigDef.Width.SHORT,
//                    MAX_RETRIES_DISPLAY
//            )
//            .define(
//                    RETRY_BACKOFF_MS,
//                    ConfigDef.Type.INT,
//                    RETRY_BACKOFF_MS_DEFAULT,
//                    NON_NEGATIVE_INT_VALIDATOR,
//                    ConfigDef.Importance.MEDIUM,
//                    RETRY_BACKOFF_MS_DOC,
//                    RETRIES_GROUP,
//                    2,
//                    ConfigDef.Width.SHORT,
//                    RETRY_BACKOFF_MS_DISPLAY
//            )
            ;


    public ArangoSinkConfig(Map<?, ?> props) {
        super(CONFIG_DEF, props);
    }

    public List<HostDescription> getEndpoints() {
        return getList(CONNECTION_ENDPOINTS).stream()
                .map(HostDescription::parse)
                .collect(Collectors.toList());
    }

    public String getUser() {
        return getString(CONNECTION_USER);
    }

    public Password getPassword() {
        return getPassword(CONNECTION_PASSWORD);
    }

    public String getDatabase() {
        return getString(CONNECTION_DATABASE);
    }

    public String getCollection() {
        return getString(CONNECTION_COLLECTION);
    }

    public com.arangodb.Protocol getProtocol() {
        Protocol protocol = Protocol.valueOf(getString(CONNECTION_PROTOCOL).toUpperCase(Locale.ROOT));
        ContentType contentType = ContentType.valueOf(getString(CONNECTION_CONTENT_TYPE).toUpperCase(Locale.ROOT));
        if (Protocol.VST.equals(protocol)) {
            return com.arangodb.Protocol.VST;
        } else if (Protocol.HTTP11.equals(protocol)) {
            if (ContentType.JSON.equals(contentType)) {
                return com.arangodb.Protocol.HTTP_JSON;
            } else {
                return com.arangodb.Protocol.HTTP_VPACK;
            }
        } else if (Protocol.HTTP2.equals(protocol)) {
            if (ContentType.JSON.equals(contentType)) {
                return com.arangodb.Protocol.HTTP2_JSON;
            } else {
                return com.arangodb.Protocol.HTTP2_VPACK;
            }
        }
        throw new IllegalArgumentException("[" + protocol + ", " + contentType + "]");
    }
}
