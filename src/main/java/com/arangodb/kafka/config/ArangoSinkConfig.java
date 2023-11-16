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

import com.arangodb.ArangoCollection;
import com.arangodb.ArangoDB;
import com.arangodb.config.HostDescription;
import com.arangodb.model.DocumentCreateOptions;
import com.arangodb.model.DocumentDeleteOptions;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.types.Password;

import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.KeyStore;
import java.security.cert.Certificate;
import java.security.cert.CertificateFactory;
import java.util.*;
import java.util.stream.Collectors;

public class ArangoSinkConfig extends AbstractConfig {
    public static final int MONITOR_REQUEST_TIMEOUT_MS = 10_000;

    public enum Protocol {
        VST,
        HTTP11,
        HTTP2
    }

    public enum ContentType {
        JSON,
        VPACK
    }

    public enum OverwriteMode {
        CONFLICT,
        IGNORE,
        REPLACE,
        UPDATE
    }

    public enum DataErrorsTolerance {
        ALL,
        NONE
    }

    //region Connection
    private static final String CONNECTION_GROUP = "Connection";
    private static final String CONNECTION_PREFIX = "connection.";

    public static final String CONNECTION_ENDPOINTS = CONNECTION_PREFIX + "endpoints";
    private static final String CONNECTION_ENDPOINTS_DOC =
            "Database connection endpoints as comma separated list of `host:port` entries.\n"
                    + "For example: `coordinator1:8529,coordinator2:8529`";
    private static final String CONNECTION_ENDPOINTS_DISPLAY = "Endpoints";

    public static final String CONNECTION_USER = CONNECTION_PREFIX + "user";
    private static final String CONNECTION_USER_DEFAULT = "root";
    private static final String CONNECTION_USER_DOC = "Database connection user.";
    private static final String CONNECTION_USER_DISPLAY = "User";

    public static final String CONNECTION_PASSWORD = CONNECTION_PREFIX + "password";
    private static final String CONNECTION_PASSWORD_DOC = "Database connection password.";
    private static final String CONNECTION_PASSWORD_DISPLAY = "Password";

    public static final String CONNECTION_DATABASE = CONNECTION_PREFIX + "database";
    private static final String CONNECTION_DATABASE_DEFAULT = "_system";
    private static final String CONNECTION_DATABASE_DOC = "Target database name.";
    private static final String CONNECTION_DATABASE_DISPLAY = "Database";

    public static final String CONNECTION_COLLECTION = CONNECTION_PREFIX + "collection";
    private static final String CONNECTION_COLLECTION_DOC = "Target collection name.";
    private static final String CONNECTION_COLLECTION_DISPLAY = "Collection";

    public static final String CONNECTION_ACQUIRE_HOST_LIST_ENABLED = CONNECTION_PREFIX + "acquireHostList.enabled";
    private static final boolean CONNECTION_ACQUIRE_HOST_LIST_ENABLED_DEFAULT = false;
    private static final String CONNECTION_ACQUIRE_HOST_LIST_ENABLED_DOC = "Periodically acquire the list of all known " +
            "ArangoDB hosts in the cluster and trigger tasks reconfiguration in case of changes.";
    private static final String CONNECTION_ACQUIRE_HOST_LIST_ENABLED_DISPLAY = "Acquire Host List";

    public static final String CONNECTION_ACQUIRE_HOST_LIST_INTERVAL_MS = CONNECTION_PREFIX + "acquireHostList.interval.ms";
    private static final int CONNECTION_ACQUIRE_HOST_LIST_INTERVAL_MS_DEFAULT = 60_000;
    private static final String CONNECTION_ACQUIRE_HOST_LIST_INTERVAL_MS_DOC = "Interval for acquiring the host list.";
    private static final String CONNECTION_ACQUIRE_HOST_LIST_INTERVAL_MS_DISPLAY = "Acquire Host List Interval";

    public static final String CONNECTION_PROTOCOL = CONNECTION_PREFIX + "protocol";
    private static final String CONNECTION_PROTOCOL_DEFAULT = Protocol.HTTP2.toString();
    private static final String CONNECTION_PROTOCOL_DOC = "Communication protocol.";
    private static final String CONNECTION_PROTOCOL_DISPLAY = "Protocol";

    public static final String CONNECTION_CONTENT_TYPE = CONNECTION_PREFIX + "content.type";
    private static final String CONNECTION_CONTENT_TYPE_DEFAULT = ContentType.JSON.toString();
    private static final String CONNECTION_CONTENT_TYPE_DOC = "Communication content type.";
    private static final String CONNECTION_CONTENT_TYPE_DISPLAY = "Content Type";
    // endregion

    // region SSL
    private static final String SSL_GROUP = "SSL";
    private static final String SSL_PREFIX = "ssl.";

    public static final String CONNECTION_SSL_ENABLED = SSL_PREFIX + "enabled";
    private static final boolean CONNECTION_SSL_ENABLED_DEFAULT = false;
    private static final String CONNECTION_SSL_ENABLED_DOC = "SSL secured driver connection.";
    private static final String CONNECTION_SSL_ENABLED_DISPLAY = "SSL enabled";

    public static final String CONNECTION_SSL_CERT_VALUE = SSL_PREFIX + "cert.value";
    private static final String CONNECTION_SSL_CERT_VALUE_DOC = "Base64 encoded SSL certificate.";
    private static final String CONNECTION_SSL_CERT_VALUE_DISPLAY = "SSL certificate";

    public static final String CONNECTION_SSL_CERT_TYPE = SSL_PREFIX + "cert.type";
    private static final String CONNECTION_SSL_CERT_TYPE_DEFAULT = "X.509";
    private static final String CONNECTION_SSL_CERT_TYPE_DOC = "Certificate type.";
    private static final String CONNECTION_SSL_CERT_TYPE_DISPLAY = "Certificate type";

    public static final String CONNECTION_SSL_CERT_ALIAS = SSL_PREFIX + "cert.alias";
    private static final String CONNECTION_SSL_CERT_ALIAS_DEFAULT = "arangodb";
    private static final String CONNECTION_SSL_CERT_ALIAS_DOC = "Certificate alias name.";
    private static final String CONNECTION_SSL_CERT_ALIAS_DISPLAY = "Certificate alias";

    public static final String CONNECTION_SSL_ALGORITHM = SSL_PREFIX + "algorithm";
    private static final String CONNECTION_SSL_ALGORITHM_DEFAULT = "SunX509";
    private static final String CONNECTION_SSL_ALGORITHM_DOC = "Trust manager algorithm.";
    private static final String CONNECTION_SSL_ALGORITHM_DISPLAY = "Trust manager algorithm";

    public static final String CONNECTION_SSL_KEYSTORE_TYPE = SSL_PREFIX + "keystore.type";
    private static final String CONNECTION_SSL_KEYSTORE_TYPE_DEFAULT = "jks";
    private static final String CONNECTION_SSL_KEYSTORE_TYPE_DOC = "Keystore type.";
    private static final String CONNECTION_SSL_KEYSTORE_TYPE_DISPLAY = "Keystore type";

    public static final String CONNECTION_SSL_PROTOCOL = SSL_PREFIX + "protocol";
    private static final String CONNECTION_SSL_PROTOCOL_DEFAULT = "TLS";
    private static final String CONNECTION_SSL_PROTOCOL_DOC = "SSLContext protocol.";
    private static final String CONNECTION_SSL_PROTOCOL_DISPLAY = "SSL protocol";

    public static final String CONNECTION_SSL_HOSTNAME_VERIFICATION = SSL_PREFIX + "hostname.verification";
    private static final boolean CONNECTION_SSL_HOSTNAME_VERIFICATION_DEFAULT = true;
    private static final String CONNECTION_SSL_HOSTNAME_VERIFICATION_DOC = "SSL hostname verification.";
    private static final String CONNECTION_SSL_HOSTNAME_VERIFICATION_DISPLAY = "SSL hostname verification";

    public static final String CONNECTION_SSL_TRUSTSTORE_LOCATION = SSL_PREFIX + "truststore.location";
    private static final String CONNECTION_SSL_TRUSTSTORE_LOCATION_DOC = "The location of the trust store file.";
    private static final String CONNECTION_SSL_TRUSTSTORE_LOCATION_DISPLAY = "Truststore location";

    public static final String CONNECTION_SSL_TRUSTSTORE_PASSWORD = SSL_PREFIX + "truststore.password";
    private static final String CONNECTION_SSL_TRUSTSTORE_PASSWORD_DOC = "The password for the trust store file.";
    private static final String CONNECTION_SSL_TRUSTSTORE_PASSWORD_DISPLAY = "Truststore password";
    //endregion

    //region Writes
    private static final String WRITES_GROUP = "Writes";

    public static final String INSERT_OVERWRITE_MODE = "insert.overwriteMode";
    private static final String INSERT_OVERWRITE_MODE_DEFAULT = OverwriteMode.CONFLICT.toString();
    private static final String INSERT_OVERWRITE_MODE_DOC =
            "The overwrite mode to use in case a document with the specified ``_key`` value already exists.\n" +
                    "Supported modes are:\n"
                    + "``conflict``: the new document value is not written and an exception is thrown.\n"
                    + "``ignore``: the new document value is not written.\n"
                    + "``replace``: the existing document is overwritten with the new document value.\n"
                    + "``update``: the existing document is patched (partially updated) with the new document\n"
                    + "            value. The behavior can be further controlled setting ``insert.mergeObjects``.";
    private static final String INSERT_OVERWRITE_MODE_DISPLAY = "Overwrite Mode";

    public static final String INSERT_MERGE_OBJECTS = "insert.mergeObjects";
    private static final boolean INSERT_MERGE_OBJECTS_DEFAULT = true;
    private static final String INSERT_MERGE_OBJECTS_DOC =
            "Whether objects (not arrays) are merged, in case ``insert.overwriteMode`` is set to ``update``:\n"
                    + "``true``: objects will be merged\n"
                    + "``false``: existing document fields will be overwritten";
    private static final String INSERT_MERGE_OBJECTS_DISPLAY = "Merge Objects";

    public static final String INSERT_TIMEOUT = "insert.timeout.ms";
    private static final int INSERT_TIMEOUT_DEFAULT = 30_000;
    private static final String INSERT_TIMEOUT_DOC = "Connect and request timeout in ms.";
    private static final String INSERT_TIMEOUT_DISPLAY = "Requests timeout";

    public static final String INSERT_WAIT_FOR_SYNC = "insert.waitForSync";
    private static final boolean INSERT_WAIT_FOR_SYNC_DEFAULT = false;
    private static final String INSERT_WAIT_FOR_SYNC_DOC =
            "Whether to wait until the documents have been synced to disk.";
    private static final String INSERT_WAIT_FOR_SYNC_DISPLAY = "WaitForSync";

    public static final String BATCH_SIZE = "batch.size";
    private static final int BATCH_SIZE_DEFAULT = 3000;
    private static final String BATCH_SIZE_DOC = "Specifies how many records to attempt to batch together for " +
            "insertion or deletion into the destination collection.";
    private static final String BATCH_SIZE_DISPLAY = "Batch Size";

    public static final String DELETE_ENABLED = "delete.enabled";
    private static final boolean DELETE_ENABLED_DEFAULT = false;
    private static final String DELETE_ENABLED_DOC = "Whether to enable delete behavior when processing tombstones.";
    private static final String DELETE_ENABLED_DISPLAY = "Enable deletes";
    //endregion

    //region error handling
    private static final String ERROR_HANDLING_GROUP = "Errors Handling";

    public static final String DATA_ERRORS_TOLERANCE = "data.errors.tolerance";
    private static final String DATA_ERRORS_TOLERANCE_DEFAULT = DataErrorsTolerance.NONE.toString();
    private static final String DATA_ERRORS_TOLERANCE_DOC =
            "Whether data errors will be tolerated:\n"
                    + "``none``: data errors will result in an immediate connector task failure\n"
                    + "``all``: data errors will be tolerated and reported to the DLQ, if configured";
    private static final String DATA_ERRORS_TOLERANCE_DISPLAY = "Data Errors Tolerance";

    public static final String DATA_ERRORS_LOG_ENABLE = "data.errors.log.enable";
    private static final boolean DATA_ERRORS_LOG_ENABLE_DEFAULT = false;
    private static final String DATA_ERRORS_LOG_ENABLE_DOC =
            "If true, write each data error and the details of the failed operation and problematic record to the Connect application log.";
    private static final String DATA_ERRORS_LOG_ENABLE_DISPLAY = "Log Data Errors";

    public static final String EXTRA_DATA_ERRORS_NUMS = "extra.data.errors.nums";
    public static final List<?> EXTRA_DATA_ERRORS_NUMS_DEFAULT = Collections.emptyList();
    public static final String EXTRA_DATA_ERRORS_NUMS_DOC = "Additional db error numbers (field `errorNum`) to consider "
            + "data errors.";
    public static final String EXTRA_DATA_ERRORS_NUMS_DISPLAY = "Extra Data Errors Numbers";
    //endregion

    //region retries
    private static final String RETRIES_GROUP = "Retries";

    public static final String MAX_RETRIES = "max.retries";
    private static final int MAX_RETRIES_DEFAULT = 10;
    private static final String MAX_RETRIES_DOC =
            "The maximum number of times to retry transient errors.";
    private static final String MAX_RETRIES_DISPLAY = "Maximum Retries";

    public static final String RETRY_BACKOFF_MS = "retry.backoff.ms";
    private static final int RETRY_BACKOFF_MS_DEFAULT = 3000;
    private static final String RETRY_BACKOFF_MS_DOC =
            "The time in milliseconds to wait following an error before a retry attempt is made.";
    private static final String RETRY_BACKOFF_MS_DISPLAY = "Retry Backoff (millis)";
    //endregion

    public static final ConfigDef CONFIG_DEF = new ConfigDef()

            //region Connection
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
                    CONNECTION_ACQUIRE_HOST_LIST_ENABLED,
                    ConfigDef.Type.BOOLEAN,
                    CONNECTION_ACQUIRE_HOST_LIST_ENABLED_DEFAULT,
                    ConfigDef.Importance.MEDIUM,
                    CONNECTION_ACQUIRE_HOST_LIST_ENABLED_DOC,
                    CONNECTION_GROUP,
                    6,
                    ConfigDef.Width.SHORT,
                    CONNECTION_ACQUIRE_HOST_LIST_ENABLED_DISPLAY
            )
            .define(
                    CONNECTION_ACQUIRE_HOST_LIST_INTERVAL_MS,
                    ConfigDef.Type.INT,
                    CONNECTION_ACQUIRE_HOST_LIST_INTERVAL_MS_DEFAULT,
                    ConfigDef.Importance.LOW,
                    CONNECTION_ACQUIRE_HOST_LIST_INTERVAL_MS_DOC,
                    CONNECTION_GROUP,
                    7,
                    ConfigDef.Width.SHORT,
                    CONNECTION_ACQUIRE_HOST_LIST_INTERVAL_MS_DISPLAY
            )
            .define(
                    CONNECTION_PROTOCOL,
                    ConfigDef.Type.STRING,
                    CONNECTION_PROTOCOL_DEFAULT,
                    new EnumValidator(Protocol.class),
                    ConfigDef.Importance.MEDIUM,
                    CONNECTION_PROTOCOL_DOC,
                    CONNECTION_GROUP,
                    8,
                    ConfigDef.Width.SHORT,
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
                    9,
                    ConfigDef.Width.SHORT,
                    CONNECTION_CONTENT_TYPE_DISPLAY,
                    new EnumRecommender(ContentType.class)
            )
            //endregion

            //region SSL
            .define(
                    CONNECTION_SSL_ENABLED,
                    ConfigDef.Type.BOOLEAN,
                    CONNECTION_SSL_ENABLED_DEFAULT,
                    ConfigDef.Importance.MEDIUM,
                    CONNECTION_SSL_ENABLED_DOC,
                    SSL_GROUP,
                    1,
                    ConfigDef.Width.SHORT,
                    CONNECTION_SSL_ENABLED_DISPLAY
            )
            .define(
                    CONNECTION_SSL_CERT_VALUE,
                    ConfigDef.Type.STRING,
                    null,
                    ConfigDef.Importance.MEDIUM,
                    CONNECTION_SSL_CERT_VALUE_DOC,
                    SSL_GROUP,
                    2,
                    ConfigDef.Width.LONG,
                    CONNECTION_SSL_CERT_VALUE_DISPLAY
            )
            .define(
                    CONNECTION_SSL_CERT_TYPE,
                    ConfigDef.Type.STRING,
                    CONNECTION_SSL_CERT_TYPE_DEFAULT,
                    ConfigDef.Importance.MEDIUM,
                    CONNECTION_SSL_CERT_TYPE_DOC,
                    SSL_GROUP,
                    3,
                    ConfigDef.Width.SHORT,
                    CONNECTION_SSL_CERT_TYPE_DISPLAY
            )
            .define(
                    CONNECTION_SSL_CERT_ALIAS,
                    ConfigDef.Type.STRING,
                    CONNECTION_SSL_CERT_ALIAS_DEFAULT,
                    ConfigDef.Importance.MEDIUM,
                    CONNECTION_SSL_CERT_ALIAS_DOC,
                    SSL_GROUP,
                    4,
                    ConfigDef.Width.MEDIUM,
                    CONNECTION_SSL_CERT_ALIAS_DISPLAY
            )
            .define(
                    CONNECTION_SSL_ALGORITHM,
                    ConfigDef.Type.STRING,
                    CONNECTION_SSL_ALGORITHM_DEFAULT,
                    ConfigDef.Importance.MEDIUM,
                    CONNECTION_SSL_ALGORITHM_DOC,
                    SSL_GROUP,
                    5,
                    ConfigDef.Width.SHORT,
                    CONNECTION_SSL_ALGORITHM_DISPLAY
            )
            .define(
                    CONNECTION_SSL_KEYSTORE_TYPE,
                    ConfigDef.Type.STRING,
                    CONNECTION_SSL_KEYSTORE_TYPE_DEFAULT,
                    ConfigDef.Importance.MEDIUM,
                    CONNECTION_SSL_KEYSTORE_TYPE_DOC,
                    SSL_GROUP,
                    6,
                    ConfigDef.Width.SHORT,
                    CONNECTION_SSL_KEYSTORE_TYPE_DISPLAY
            )
            .define(
                    CONNECTION_SSL_PROTOCOL,
                    ConfigDef.Type.STRING,
                    CONNECTION_SSL_PROTOCOL_DEFAULT,
                    ConfigDef.Importance.MEDIUM,
                    CONNECTION_SSL_PROTOCOL_DOC,
                    SSL_GROUP,
                    7,
                    ConfigDef.Width.SHORT,
                    CONNECTION_SSL_PROTOCOL_DISPLAY
            )
            .define(
                    CONNECTION_SSL_HOSTNAME_VERIFICATION,
                    ConfigDef.Type.BOOLEAN,
                    CONNECTION_SSL_HOSTNAME_VERIFICATION_DEFAULT,
                    ConfigDef.Importance.MEDIUM,
                    CONNECTION_SSL_HOSTNAME_VERIFICATION_DOC,
                    SSL_GROUP,
                    8,
                    ConfigDef.Width.SHORT,
                    CONNECTION_SSL_HOSTNAME_VERIFICATION_DISPLAY
            )
            .define(
                    CONNECTION_SSL_TRUSTSTORE_LOCATION,
                    ConfigDef.Type.STRING,
                    null,
                    ConfigDef.Importance.MEDIUM,
                    CONNECTION_SSL_TRUSTSTORE_LOCATION_DOC,
                    SSL_GROUP,
                    9,
                    ConfigDef.Width.LONG,
                    CONNECTION_SSL_TRUSTSTORE_LOCATION_DISPLAY
            )
            .define(
                    CONNECTION_SSL_TRUSTSTORE_PASSWORD,
                    ConfigDef.Type.PASSWORD,
                    null,
                    ConfigDef.Importance.MEDIUM,
                    CONNECTION_SSL_TRUSTSTORE_PASSWORD_DOC,
                    SSL_GROUP,
                    10,
                    ConfigDef.Width.MEDIUM,
                    CONNECTION_SSL_TRUSTSTORE_PASSWORD_DISPLAY
            )
            //endregion

            //region Writes
            .define(
                    INSERT_OVERWRITE_MODE,
                    ConfigDef.Type.STRING,
                    INSERT_OVERWRITE_MODE_DEFAULT,
                    new EnumValidator(OverwriteMode.class),
                    ConfigDef.Importance.HIGH,
                    INSERT_OVERWRITE_MODE_DOC,
                    WRITES_GROUP,
                    1,
                    ConfigDef.Width.MEDIUM,
                    INSERT_OVERWRITE_MODE_DISPLAY,
                    new EnumRecommender(OverwriteMode.class)
            )
            .define(
                    INSERT_MERGE_OBJECTS,
                    ConfigDef.Type.BOOLEAN,
                    INSERT_MERGE_OBJECTS_DEFAULT,
                    ConfigDef.Importance.MEDIUM,
                    INSERT_MERGE_OBJECTS_DOC,
                    WRITES_GROUP,
                    2,
                    ConfigDef.Width.SHORT,
                    INSERT_MERGE_OBJECTS_DISPLAY
            )
            .define(
                    BATCH_SIZE,
                    ConfigDef.Type.INT,
                    BATCH_SIZE_DEFAULT,
                    ConfigDef.Importance.MEDIUM,
                    BATCH_SIZE_DOC,
                    WRITES_GROUP,
                    3,
                    ConfigDef.Width.SHORT,
                    BATCH_SIZE_DISPLAY
            )
            .define(
                    DELETE_ENABLED,
                    ConfigDef.Type.BOOLEAN,
                    DELETE_ENABLED_DEFAULT,
                    ConfigDef.Importance.MEDIUM,
                    DELETE_ENABLED_DOC,
                    WRITES_GROUP,
                    4,
                    ConfigDef.Width.SHORT,
                    DELETE_ENABLED_DISPLAY
            )
            .define(
                    INSERT_TIMEOUT,
                    ConfigDef.Type.INT,
                    INSERT_TIMEOUT_DEFAULT,
                    ConfigDef.Importance.LOW,
                    INSERT_TIMEOUT_DOC,
                    WRITES_GROUP,
                    5,
                    ConfigDef.Width.SHORT,
                    INSERT_TIMEOUT_DISPLAY
            )
            .define(
                    INSERT_WAIT_FOR_SYNC,
                    ConfigDef.Type.BOOLEAN,
                    INSERT_WAIT_FOR_SYNC_DEFAULT,
                    ConfigDef.Importance.LOW,
                    INSERT_WAIT_FOR_SYNC_DOC,
                    WRITES_GROUP,
                    6,
                    ConfigDef.Width.SHORT,
                    INSERT_WAIT_FOR_SYNC_DISPLAY
            )
            //endregion

            // region error handling
            .define(
                    DATA_ERRORS_TOLERANCE,
                    ConfigDef.Type.STRING,
                    DATA_ERRORS_TOLERANCE_DEFAULT,
                    new EnumValidator(DataErrorsTolerance.class),
                    ConfigDef.Importance.MEDIUM,
                    DATA_ERRORS_TOLERANCE_DOC,
                    ERROR_HANDLING_GROUP,
                    1,
                    ConfigDef.Width.MEDIUM,
                    DATA_ERRORS_TOLERANCE_DISPLAY,
                    new EnumRecommender(DataErrorsTolerance.class)
            )
            .define(
                    DATA_ERRORS_LOG_ENABLE,
                    ConfigDef.Type.BOOLEAN,
                    DATA_ERRORS_LOG_ENABLE_DEFAULT,
                    ConfigDef.Importance.LOW,
                    DATA_ERRORS_LOG_ENABLE_DOC,
                    ERROR_HANDLING_GROUP,
                    2,
                    ConfigDef.Width.SHORT,
                    DATA_ERRORS_LOG_ENABLE_DISPLAY
            )
            .define(
                    EXTRA_DATA_ERRORS_NUMS,
                    ConfigDef.Type.LIST,
                    EXTRA_DATA_ERRORS_NUMS_DEFAULT,
                    ConfigDef.Importance.LOW,
                    EXTRA_DATA_ERRORS_NUMS_DOC,
                    ERROR_HANDLING_GROUP,
                    3,
                    ConfigDef.Width.MEDIUM,
                    EXTRA_DATA_ERRORS_NUMS_DISPLAY
            )
            //endregion

            // region retries
            .define(
                    MAX_RETRIES,
                    ConfigDef.Type.INT,
                    MAX_RETRIES_DEFAULT,
                    ConfigDef.Importance.MEDIUM,
                    MAX_RETRIES_DOC,
                    RETRIES_GROUP,
                    1,
                    ConfigDef.Width.SHORT,
                    MAX_RETRIES_DISPLAY
            )
            .define(
                    RETRY_BACKOFF_MS,
                    ConfigDef.Type.INT,
                    RETRY_BACKOFF_MS_DEFAULT,
                    ConfigDef.Importance.MEDIUM,
                    RETRY_BACKOFF_MS_DOC,
                    RETRIES_GROUP,
                    2,
                    ConfigDef.Width.SHORT,
                    RETRY_BACKOFF_MS_DISPLAY
            )
            //endregion
            ;


    public ArangoSinkConfig(Map<?, ?> props) {
        super(CONFIG_DEF, props);
        ensureValidSslConfig();
    }

    public Protocol getConnectionProtocol() {
        return Protocol.valueOf(getString(CONNECTION_PROTOCOL).toUpperCase(Locale.ROOT));
    }

    private com.arangodb.Protocol getProtocol() {
        Protocol protocol = getConnectionProtocol();
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
        throw new ConfigException("[" + CONNECTION_PROTOCOL + "=" + protocol + ", " +
                CONNECTION_CONTENT_TYPE + "=" + contentType + "] is not supported.");
    }

    private SSLContext createSslContext() {
        String certValue = getString(CONNECTION_SSL_CERT_VALUE);
        String sslCertType = getString(CONNECTION_SSL_CERT_TYPE);
        String sslKeystoreType = getString(CONNECTION_SSL_KEYSTORE_TYPE);
        String sslCertAlias = getString(CONNECTION_SSL_CERT_ALIAS);
        String sslAlgorithm = getString(CONNECTION_SSL_ALGORITHM);
        String sslProtocol = getString(CONNECTION_SSL_PROTOCOL);
        String trustStoreLocation = getString(CONNECTION_SSL_TRUSTSTORE_LOCATION);
        Password trustStorePassword = getPassword(CONNECTION_SSL_TRUSTSTORE_PASSWORD);

        try {
            KeyStore ks = KeyStore.getInstance(sslKeystoreType);
            if (certValue != null) {
                ByteArrayInputStream is = new ByteArrayInputStream(Base64.getDecoder().decode(certValue));
                Certificate cert = CertificateFactory.getInstance(sslCertType).generateCertificate(is);
                ks.load(null);
                ks.setCertificateEntry(sslCertAlias, cert);
            } else if (trustStoreLocation != null) {
                DataInputStream stream = new DataInputStream(Files.newInputStream(Paths.get(trustStoreLocation)));
                ks.load(stream, trustStorePassword.value().toCharArray());
            } else {
                return SSLContext.getDefault();
            }

            TrustManagerFactory tmf = TrustManagerFactory.getInstance(sslAlgorithm);
            tmf.init(ks);
            SSLContext sc = SSLContext.getInstance(sslProtocol);
            sc.init(null, tmf.getTrustManagers(), null);
            return sc;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private ArangoDB.Builder createAdbBuilder() {
        Password passwd = getPassword(CONNECTION_PASSWORD);
        ArangoDB.Builder builder = new ArangoDB.Builder()
                .user(getString(CONNECTION_USER))
                .protocol(getProtocol());
        if (passwd != null) {
            builder.password(passwd.value());
        }
        for (HostDescription ep : getEndpoints()) {
            builder.host(ep.getHost(), ep.getPort());
        }
        if (getBoolean(CONNECTION_SSL_ENABLED)) {
            builder
                    .useSsl(true)
                    .sslContext(createSslContext())
                    .verifyHost(getBoolean(CONNECTION_SSL_HOSTNAME_VERIFICATION));
        }
        return builder;
    }

    public ArangoDB createMonitorClient() {
        return createAdbBuilder()
                .timeout(MONITOR_REQUEST_TIMEOUT_MS)
                .acquireHostList(true)
                .acquireHostListInterval(getInt(CONNECTION_ACQUIRE_HOST_LIST_INTERVAL_MS))
                .build();
    }

    public ArangoCollection createCollection() {
        return createAdbBuilder()
                .timeout(getInt(INSERT_TIMEOUT))
                .build()
                .db(getString(CONNECTION_DATABASE))
                .collection(getString(CONNECTION_COLLECTION));
    }

    public DocumentCreateOptions getCreateOptions() {
        return new DocumentCreateOptions()
                .overwriteMode(com.arangodb.model.OverwriteMode.valueOf(
                        getString(INSERT_OVERWRITE_MODE).toLowerCase(Locale.ROOT)
                ))
                .mergeObjects(getBoolean(INSERT_MERGE_OBJECTS))
                .keepNull(true)
                .silent(true)
                .refillIndexCaches(false)
                .waitForSync(getBoolean(INSERT_WAIT_FOR_SYNC));
    }

    public DocumentDeleteOptions getDeleteOptions() {
        return new DocumentDeleteOptions()
                .silent(true)
                .refillIndexCaches(false)
                .waitForSync(getBoolean(INSERT_WAIT_FOR_SYNC));
    }

    public int getBatchSize() {
        return getInt(BATCH_SIZE);
    }

    public boolean isDeleteEnabled() {
        return getBoolean(DELETE_ENABLED);
    }

    public int getMaxRetries() {
        return getInt(MAX_RETRIES);
    }

    public int getRetryBackoffMs() {
        return getInt(RETRY_BACKOFF_MS);
    }

    public boolean getTolerateDataErrors() {
        DataErrorsTolerance value = DataErrorsTolerance.valueOf(
                getString(DATA_ERRORS_TOLERANCE).toUpperCase(Locale.ROOT));
        return DataErrorsTolerance.ALL.equals(value);
    }

    public boolean getLogDataErrors() {
        return getBoolean(DATA_ERRORS_LOG_ENABLE);
    }

    public String getUser() {
        return getString(CONNECTION_USER);
    }

    public boolean isAcquireHostListEnabled() {
        return getBoolean(CONNECTION_ACQUIRE_HOST_LIST_ENABLED);
    }

    public int getAcquireHostIntervalMs() {
        return getInt(CONNECTION_ACQUIRE_HOST_LIST_INTERVAL_MS);
    }

    public Set<HostDescription> getEndpoints() {
        return getList(CONNECTION_ENDPOINTS).stream()
                .map(HostDescription::parse)
                .collect(Collectors.toSet());
    }

    public Set<Integer> getExtraDataErrorsNums() {
        return getList(EXTRA_DATA_ERRORS_NUMS).stream()
                .map(Integer::parseInt)
                .collect(Collectors.toSet());
    }

    private void ensureValidSslConfig() {
        String certValue = getString(CONNECTION_SSL_CERT_VALUE);
        String trustStoreLocation = getString(CONNECTION_SSL_TRUSTSTORE_LOCATION);
        if (certValue != null && trustStoreLocation != null) {
            throw new ConfigException("Cannot set both " + CONNECTION_SSL_CERT_VALUE + " and " + CONNECTION_SSL_TRUSTSTORE_LOCATION);
        }
    }

}
