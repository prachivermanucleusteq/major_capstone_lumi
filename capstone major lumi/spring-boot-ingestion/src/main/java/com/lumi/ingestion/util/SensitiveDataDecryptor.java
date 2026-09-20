package com.lumi.ingestion.util;

import javax.crypto.Cipher;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Base64;

public final class SensitiveDataDecryptor {

    private static final String ALGORITHM = "AES/GCM/NoPadding";
    private static final String KEY_ALGORITHM = "AES";
    private static final int KEY_BYTES = 32;
    private static final int IV_BYTES = 12;
    private static final int TAG_BITS = 128;

    private SensitiveDataDecryptor() {
    }

    public static String decrypt(String ciphertext) {
        if (ciphertext == null || ciphertext.isBlank()) {
            return ciphertext;
        }

        try {
            String rawKey = System.getenv("LUMI_ENCRYPTION_KEY");

            if (rawKey == null || rawKey.isBlank()) {
                throw new IllegalStateException("LUMI_ENCRYPTION_KEY environment variable is not configured");
            }

            byte[] keyBytes = rawKey.getBytes(StandardCharsets.UTF_8);

            if (keyBytes.length != KEY_BYTES) {
                throw new IllegalStateException("LUMI_ENCRYPTION_KEY must be exactly 32 bytes (got " + keyBytes.length + ")");
            }

            byte[] combined = Base64.getDecoder().decode(ciphertext);

            if (combined.length <= IV_BYTES) {
                throw new IllegalArgumentException("Invalid encrypted value");
            }

            byte[] iv = Arrays.copyOfRange(combined, 0, IV_BYTES);
            byte[] encrypted = Arrays.copyOfRange(combined, IV_BYTES, combined.length);
            Cipher cipher = Cipher.getInstance(ALGORITHM);
            cipher.init(Cipher.DECRYPT_MODE, new SecretKeySpec(keyBytes, KEY_ALGORITHM), new GCMParameterSpec(TAG_BITS, iv));
            byte[] decrypted = cipher.doFinal(encrypted);
            return new String(decrypted, StandardCharsets.UTF_8
            );

        } catch (Exception e) {
            throw new IllegalStateException("Unable to decrypt sensitive data", e);
        }
    }
}