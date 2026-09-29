package com.lumi.ingestion.util;

import org.junit.jupiter.api.Test;

import javax.crypto.Cipher;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.util.Base64;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

class SensitiveDataDecryptorTest {

    private static final String KEY = "0123456789abcdef0123456789abcdef";

    @Test
    void decrypt_returnsNullAndBlankValuesUnchanged() {
        SensitiveDataDecryptor decryptor = new SensitiveDataDecryptor(KEY);

        assertNull(decryptor.decrypt(null));
        assertEquals("  ", decryptor.decrypt("  "));
    }

    @Test
    void decrypt_returnsPlaintextForValidCiphertext() throws Exception {
        SensitiveDataDecryptor decryptor = new SensitiveDataDecryptor(KEY);

        assertEquals("555-0100", decryptor.decrypt(encrypt("555-0100")));
    }

    @Test
    void decrypt_rejectsMissingOrIncorrectKey() {
        assertThrows(IllegalStateException.class,
                () -> new SensitiveDataDecryptor("").decrypt("ciphertext"));
        assertThrows(IllegalStateException.class,
                () -> new SensitiveDataDecryptor("short").decrypt("ciphertext"));
    }

    @Test
    void decrypt_rejectsMalformedCiphertext() {
        SensitiveDataDecryptor decryptor = new SensitiveDataDecryptor(KEY);

        assertThrows(IllegalStateException.class, () -> decryptor.decrypt("not-base64"));
        assertThrows(IllegalStateException.class,
                () -> decryptor.decrypt(Base64.getEncoder().encodeToString(new byte[12])));
    }

    private String encrypt(String plaintext) throws Exception {
        byte[] iv = new byte[12];
        new SecureRandom().nextBytes(iv);
        Cipher cipher = Cipher.getInstance("AES/GCM/NoPadding");
        cipher.init(Cipher.ENCRYPT_MODE,
                new SecretKeySpec(KEY.getBytes(StandardCharsets.UTF_8), "AES"),
                new GCMParameterSpec(128, iv));
        byte[] encrypted = cipher.doFinal(plaintext.getBytes(StandardCharsets.UTF_8));
        byte[] combined = ByteBuffer.allocate(iv.length + encrypted.length)
                .put(iv)
                .put(encrypted)
                .array();
        return Base64.getEncoder().encodeToString(combined);
    }
}