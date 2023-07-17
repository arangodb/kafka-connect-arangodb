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

import java.util.Map;

public class SslTarget extends JsonTarget {
    private final static String certValue = "MIIDezCCAmOgAwIBAgIEeDCzXzANBgkqhkiG9w0BAQsFADBuMRAwDgYDVQQGEwdVbmtub3duMRAwDgYDVQQIEwdVbmtub3duMRAwDgYDVQQHEwdVbmtub3duMRAwDgYDVQQKEwdVbmtub3duMRAwDgYDVQQLEwdVbmtub3duMRIwEAYDVQQDEwlsb2NhbGhvc3QwHhcNMjAxMTAxMTg1MTE5WhcNMzAxMDMwMTg1MTE5WjBuMRAwDgYDVQQGEwdVbmtub3duMRAwDgYDVQQIEwdVbmtub3duMRAwDgYDVQQHEwdVbmtub3duMRAwDgYDVQQKEwdVbmtub3duMRAwDgYDVQQLEwdVbmtub3duMRIwEAYDVQQDEwlsb2NhbGhvc3QwggEiMA0GCSqGSIb3DQEBAQUAA4IBDwAwggEKAoIBAQC1WiDnd4+uCmMG539ZNZB8NwI0RZF3sUSQGPx3lkqaFTZVEzMZL76HYvdc9Qg7difyKyQ09RLSpMALX9euSseD7bZGnfQH52BnKcT09eQ3wh7aVQ5sN2omygdHLC7X9usntxAfv7NzmvdogNXoJQyY/hSZff7RIqWH8NnAUKkjqOe6Bf5LDbxHKESmrFBxOCOnhcpvZWetwpiRdJVPwUn5P82CAZzfiBfmBZnB7D0l+/6Cv4jMuH26uAIcixnVekBQzl1RgwczuiZf2MGO64vDMMJJWE9ClZF1uQuQrwXF6qwhuP1Hnkii6wNbTtPWlGSkqeutr004+Hzbf8KnRY4PAgMBAAGjITAfMB0GA1UdDgQWBBTBrv9Awynt3C5IbaCNyOW5v4DNkTANBgkqhkiG9w0BAQsFAAOCAQEAIm9rPvDkYpmzpSIhR3VXG9Y71gxRDrqkEeLsMoEyqGnw/zx1bDCNeGg2PncLlW6zTIipEBooixIE9U7KxHgZxBy0Et6EEWvIUmnr6F4F+dbTD050GHlcZ7eOeqYTPYeQC502G1Fo4tdNi4lDP9L9XZpf7Q1QimRH2qaLS03ZFZa2tY7ah/RQqZL8Dkxx8/zc25sgTHVpxoK853glBVBs/ENMiyGJWmAXQayewY3EPt/9wGwV4KmU3dPDleQeXSUGPUISeQxFjy+jCw21pYviWVJTNBA9l5ny3GhEmcnOT/gQHCvVRLyGLMbaMZ4JrPwb+aAtBgrgeiK4xeSMMvrbhw==";

    public SslTarget(String name) {
        super(name);
    }

    @Override
    public Map<String, String> getConfig() {
        Map<String, String> cfg = super.getConfig();
        cfg.put(ArangoSinkConfig.CONNECTION_SSL_ENABLED, "true");
        cfg.put(ArangoSinkConfig.CONNECTION_SSL_CERT_VALUE, certValue);
        cfg.put(ArangoSinkConfig.CONNECTION_SSL_HOSTNAME_VERIFICATION, "false");
        return cfg;
    }

}
