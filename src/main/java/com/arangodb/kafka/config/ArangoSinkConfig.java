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
import java.util.Base64;
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

    public enum OverwriteMode {
        CONFLICT,
        IGNORE,
        REPLACE,
        UPDATE
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

    public static final String CONNECTION_PROTOCOL = CONNECTION_PREFIX + "protocol";
    private static final String CONNECTION_PROTOCOL_DEFAULT = Protocol.HTTP2.toString();
    private static final String CONNECTION_PROTOCOL_DOC = "Communication protocol.";
    private static final String CONNECTION_PROTOCOL_DISPLAY = "Protocol";

    public static final String CONNECTION_CONTENT_TYPE = CONNECTION_PREFIX + "content-type";
    private static final String CONNECTION_CONTENT_TYPE_DEFAULT = ContentType.JSON.toString();
    private static final String CONNECTION_CONTENT_TYPE_DOC = "Communication content type.";
    private static final String CONNECTION_CONTENT_TYPE_DISPLAY = "Content Type";

    public static final String CONNECTION_SSL_ENABLED = CONNECTION_PREFIX + "ssl.enabled";
    private static final String CONNECTION_SSL_ENABLED_DEFAULT = "false";
    private static final String CONNECTION_SSL_ENABLED_DOC = "SSL secured driver connection.";
    private static final String CONNECTION_SSL_ENABLED_DISPLAY = "SSL enabled";

    public static final String CONNECTION_SSL_CERT_VALUE = CONNECTION_PREFIX + "ssl.cert.value";
    private static final String CONNECTION_SSL_CERT_VALUE_DOC = "Base64 encoded SSL certificate.";
    private static final String CONNECTION_SSL_CERT_VALUE_DISPLAY = "SSL certificate";

    public static final String CONNECTION_SSL_CERT_TYPE = CONNECTION_PREFIX + "ssl.cert.type";
    private static final String CONNECTION_SSL_CERT_TYPE_DEFAULT = "X.509";
    private static final String CONNECTION_SSL_CERT_TYPE_DOC = "Certificate type.";
    private static final String CONNECTION_SSL_CERT_TYPE_DISPLAY = "Certificate type";

    public static final String CONNECTION_SSL_CERT_ALIAS = CONNECTION_PREFIX + "ssl.cert.alias";
    private static final String CONNECTION_SSL_CERT_ALIAS_DEFAULT = "arangodb";
    private static final String CONNECTION_SSL_CERT_ALIAS_DOC = "Certificate alias name.";
    private static final String CONNECTION_SSL_CERT_ALIAS_DISPLAY = "Certificate alias";

    public static final String CONNECTION_SSL_ALGORITHM = CONNECTION_PREFIX + "ssl.algorithm";
    private static final String CONNECTION_SSL_ALGORITHM_DEFAULT = "SunX509";
    private static final String CONNECTION_SSL_ALGORITHM_DOC = "Trust manager algorithm.";
    private static final String CONNECTION_SSL_ALGORITHM_DISPLAY = "Trust manager algorithm";

    public static final String CONNECTION_SSL_KEYSTORE_TYPE = CONNECTION_PREFIX + "ssl.keystore.type";
    private static final String CONNECTION_SSL_KEYSTORE_TYPE_DEFAULT = "jks";
    private static final String CONNECTION_SSL_KEYSTORE_TYPE_DOC = "Keystore type.";
    private static final String CONNECTION_SSL_KEYSTORE_TYPE_DISPLAY = "Keystore type";

    public static final String CONNECTION_SSL_PROTOCOL = CONNECTION_PREFIX + "ssl.protocol";
    private static final String CONNECTION_SSL_PROTOCOL_DEFAULT = "TLS";
    private static final String CONNECTION_SSL_PROTOCOL_DOC = "SSLContext protocol.";
    private static final String CONNECTION_SSL_PROTOCOL_DISPLAY = "SSL protocol";

    public static final String CONNECTION_SSL_HOSTNAME_VERIFICATION = CONNECTION_PREFIX + "ssl.hostname.verification";
    private static final String CONNECTION_SSL_HOSTNAME_VERIFICATION_DEFAULT = "true";
    private static final String CONNECTION_SSL_HOSTNAME_VERIFICATION_DOC = "SSL hostname verification.";
    private static final String CONNECTION_SSL_HOSTNAME_VERIFICATION_DISPLAY = "SSL hostname verification";

    public static final String CONNECTION_SSL_TRUSTSTORE_LOCATION = CONNECTION_PREFIX + "ssl.truststore.location";
    private static final String CONNECTION_SSL_TRUSTSTORE_LOCATION_DOC = "The location of the trust store file.";
    private static final String CONNECTION_SSL_TRUSTSTORE_LOCATION_DISPLAY = "Truststore location";

    public static final String CONNECTION_SSL_TRUSTSTORE_PASSWORD = CONNECTION_PREFIX + "ssl.truststore.password";
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
    private static final String INSERT_MERGE_OBJECTS_DEFAULT = "true";
    private static final String INSERT_MERGE_OBJECTS_DOC =
            "Whether objects (not arrays) are merged, in case ``insert.overwriteMode`` is set to ``update``:\n"
                    + "``true``: objects will be merged\n"
                    + "``false``: existing document fields will be overwritten";
    private static final String INSERT_MERGE_OBJECTS_DISPLAY = "Merge Objects";
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
                    CONNECTION_PROTOCOL,
                    ConfigDef.Type.STRING,
                    CONNECTION_PROTOCOL_DEFAULT,
                    new EnumValidator(Protocol.class),
                    ConfigDef.Importance.MEDIUM,
                    CONNECTION_PROTOCOL_DOC,
                    CONNECTION_GROUP,
                    6,
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
                    7,
                    ConfigDef.Width.SHORT,
                    CONNECTION_CONTENT_TYPE_DISPLAY,
                    new EnumRecommender(ContentType.class)
            )
            .define(
                    CONNECTION_SSL_ENABLED,
                    ConfigDef.Type.BOOLEAN,
                    CONNECTION_SSL_ENABLED_DEFAULT,
                    ConfigDef.Importance.MEDIUM,
                    CONNECTION_SSL_ENABLED_DOC,
                    CONNECTION_GROUP,
                    8,
                    ConfigDef.Width.SHORT,
                    CONNECTION_SSL_ENABLED_DISPLAY
            )
            .define(
                    CONNECTION_SSL_CERT_VALUE,
                    ConfigDef.Type.STRING,
                    null,
                    ConfigDef.Importance.MEDIUM,
                    CONNECTION_SSL_CERT_VALUE_DOC,
                    CONNECTION_GROUP,
                    9,
                    ConfigDef.Width.LONG,
                    CONNECTION_SSL_CERT_VALUE_DISPLAY
            )
            .define(
                    CONNECTION_SSL_CERT_TYPE,
                    ConfigDef.Type.STRING,
                    CONNECTION_SSL_CERT_TYPE_DEFAULT,
                    ConfigDef.Importance.MEDIUM,
                    CONNECTION_SSL_CERT_TYPE_DOC,
                    CONNECTION_GROUP,
                    10,
                    ConfigDef.Width.SHORT,
                    CONNECTION_SSL_CERT_TYPE_DISPLAY
            )
            .define(
                    CONNECTION_SSL_CERT_ALIAS,
                    ConfigDef.Type.STRING,
                    CONNECTION_SSL_CERT_ALIAS_DEFAULT,
                    ConfigDef.Importance.MEDIUM,
                    CONNECTION_SSL_CERT_ALIAS_DOC,
                    CONNECTION_GROUP,
                    11,
                    ConfigDef.Width.MEDIUM,
                    CONNECTION_SSL_CERT_ALIAS_DISPLAY
            )
            .define(
                    CONNECTION_SSL_ALGORITHM,
                    ConfigDef.Type.STRING,
                    CONNECTION_SSL_ALGORITHM_DEFAULT,
                    ConfigDef.Importance.MEDIUM,
                    CONNECTION_SSL_ALGORITHM_DOC,
                    CONNECTION_GROUP,
                    12,
                    ConfigDef.Width.SHORT,
                    CONNECTION_SSL_ALGORITHM_DISPLAY
            )
            .define(
                    CONNECTION_SSL_KEYSTORE_TYPE,
                    ConfigDef.Type.STRING,
                    CONNECTION_SSL_KEYSTORE_TYPE_DEFAULT,
                    ConfigDef.Importance.MEDIUM,
                    CONNECTION_SSL_KEYSTORE_TYPE_DOC,
                    CONNECTION_GROUP,
                    13,
                    ConfigDef.Width.SHORT,
                    CONNECTION_SSL_KEYSTORE_TYPE_DISPLAY
            )
            .define(
                    CONNECTION_SSL_PROTOCOL,
                    ConfigDef.Type.STRING,
                    CONNECTION_SSL_PROTOCOL_DEFAULT,
                    ConfigDef.Importance.MEDIUM,
                    CONNECTION_SSL_PROTOCOL_DOC,
                    CONNECTION_GROUP,
                    14,
                    ConfigDef.Width.SHORT,
                    CONNECTION_SSL_PROTOCOL_DISPLAY
            )
            .define(
                    CONNECTION_SSL_HOSTNAME_VERIFICATION,
                    ConfigDef.Type.BOOLEAN,
                    CONNECTION_SSL_HOSTNAME_VERIFICATION_DEFAULT,
                    ConfigDef.Importance.MEDIUM,
                    CONNECTION_SSL_HOSTNAME_VERIFICATION_DOC,
                    CONNECTION_GROUP,
                    15,
                    ConfigDef.Width.SHORT,
                    CONNECTION_SSL_HOSTNAME_VERIFICATION_DISPLAY
            )
            .define(
                    CONNECTION_SSL_TRUSTSTORE_LOCATION,
                    ConfigDef.Type.STRING,
                    null,
                    ConfigDef.Importance.MEDIUM,
                    CONNECTION_SSL_TRUSTSTORE_LOCATION_DOC,
                    CONNECTION_GROUP,
                    16,
                    ConfigDef.Width.LONG,
                    CONNECTION_SSL_TRUSTSTORE_LOCATION_DISPLAY
            )
            .define(
                    CONNECTION_SSL_TRUSTSTORE_PASSWORD,
                    ConfigDef.Type.PASSWORD,
                    null,
                    ConfigDef.Importance.MEDIUM,
                    CONNECTION_SSL_TRUSTSTORE_PASSWORD_DOC,
                    CONNECTION_GROUP,
                    17,
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
            //endregion


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
        ensureValidSslConfig();
    }

    private com.arangodb.Protocol getProtocol() {
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

    public ArangoCollection createCollection() {
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
        return builder.build()
                .db(getString(CONNECTION_DATABASE))
                .collection(getString(CONNECTION_COLLECTION));
    }

    public DocumentCreateOptions createInsertOptions() {
        return new DocumentCreateOptions()
                .overwriteMode(com.arangodb.model.OverwriteMode.valueOf(
                        getString(INSERT_OVERWRITE_MODE).toLowerCase(Locale.ROOT)
                ))
                .mergeObjects(getBoolean(INSERT_MERGE_OBJECTS));
    }

    List<HostDescription> getEndpoints() {
        return getList(CONNECTION_ENDPOINTS).stream()
                .map(HostDescription::parse)
                .collect(Collectors.toList());
    }

    private void ensureValidSslConfig() {
        String certValue = getString(CONNECTION_SSL_CERT_VALUE);
        String trustStoreLocation = getString(CONNECTION_SSL_TRUSTSTORE_LOCATION);
        if (certValue != null && trustStoreLocation != null) {
            throw new ConfigException("Cannot set both " + CONNECTION_SSL_CERT_VALUE + " and " + CONNECTION_SSL_TRUSTSTORE_LOCATION);
        }
    }

}
