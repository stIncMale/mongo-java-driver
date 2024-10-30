package com.lodh.myproduct.myapp;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;
import java.util.Map;

import com.mongodb.AutoEncryptionSettings;
import com.mongodb.MongoClientSettings;

public class MyCustomizer {
    private static final String KEY_VAULT_NAMESPACE = "encryption_CRMGATEWAY.keyVault";
    private static final String KMS_NAME = "local";

    private byte[] getMasterKey() {
        return getMasterKey(List.of(
                Base64.getEncoder().encodeToString("a".repeat(96).getBytes(StandardCharsets.UTF_8))));
    }

    private byte[] getMasterKey(List<String> transitKeys) {
        final Base64.Decoder base64Decoder = Base64.getDecoder();
        final ByteArrayOutputStream keyOutputStream = new ByteArrayOutputStream();
        transitKeys.forEach(key -> keyOutputStream.writeBytes(base64Decoder.decode(key)));
        return keyOutputStream.toByteArray();
    }

    public MongoClientSettings.Builder customize(MongoClientSettings.Builder builder) {
        Map<String, Map<String, Object>> kmsProviders = getKmsProviders();
        return builder.autoEncryptionSettings(AutoEncryptionSettings.builder()
                .keyVaultNamespace(KEY_VAULT_NAMESPACE)
                .kmsProviders(kmsProviders)
                .bypassAutoEncryption(true)
                .build());
    }

    Map<String, Map<String, Object>> getKmsProviders() {
        final Map<String, Object> keyMap = Map.of("key", getMasterKey());
        return Map.of(KMS_NAME, keyMap);
    }
}
