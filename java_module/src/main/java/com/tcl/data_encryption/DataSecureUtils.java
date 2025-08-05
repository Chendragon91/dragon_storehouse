package com.tcl.data_encryption;
import javax.crypto.Cipher;
import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;
import javax.crypto.spec.GCMParameterSpec;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;

public class DataSecureUtils {
    private static final String AES_ALGORITHM = "AES/GCM/NoPadding";
    private static final int GCM_IV_LENGTH = 12;
    private static final int GCM_TAG_LENGTH = 16;
    private static final int AES_KEY_SIZE = 256;

    private final SecretKey secretKey;
    public static final byte[] keyBytes = DataSecureUtils.hexStringToByteArray("00112233445566778899AABBCCDDEEFF0123456789ABCDEF02468ACE13579BDF");


    public DataSecureUtils(SecretKey secretKey) {
        // 初始化密钥
        this.secretKey = (secretKey != null) ? secretKey : generateSecretKey();
    }

    /**
     * 生成AES密钥
     * @return 生成的密钥
     */
    public static SecretKey generateSecretKey() {
        try {
            KeyGenerator keyGenerator = KeyGenerator.getInstance("AES");
            keyGenerator.init(AES_KEY_SIZE);
            return keyGenerator.generateKey();
        } catch (Exception e) {
            throw new RuntimeException("生成AES密钥失败", e);
        }
    }

    /**
     * 加密JSON字符串数据
     * @param jsonData 待加密的JSON字符串
     * @return 加密后的数据(IV+密文+标签)
     */
    public byte[] encrypt(String jsonData) {
        try {
            byte[] plaintext = jsonData.getBytes(StandardCharsets.UTF_8);

            // 生成随机IV
            byte[] iv = new byte[GCM_IV_LENGTH];
            SecureRandom random = new SecureRandom();
            random.nextBytes(iv);

            // 初始化加密器
            Cipher cipher = Cipher.getInstance(AES_ALGORITHM);
            GCMParameterSpec parameterSpec = new GCMParameterSpec(GCM_TAG_LENGTH * 8, iv);
            cipher.init(Cipher.ENCRYPT_MODE, secretKey, parameterSpec);

            // 加密(包含自动生成的认证标签)
            byte[] ciphertext = cipher.doFinal(plaintext);

            // 组合IV和加密数据(IV + 密文+标签)
            return ByteBuffer.allocate(iv.length + ciphertext.length)
                    .put(iv)
                    .put(ciphertext)
                    .array();
        } catch (Exception e) {
            throw new RuntimeException("加密数据失败", e);
        }
    }

    /**
     * 解密数据为JSON字符串
     * @param encryptedData 加密的数据(IV+密文+标签)
     * @return 解密后的JSON字符串
     */
    public String decrypt(byte[] encryptedData) {
        try {
            // 分离IV和密文(包含标签)
            ByteBuffer byteBuffer = ByteBuffer.wrap(encryptedData);
            byte[] iv = new byte[GCM_IV_LENGTH];
            byteBuffer.get(iv);

            byte[] ciphertext = new byte[byteBuffer.remaining()];
            byteBuffer.get(ciphertext);

            // 初始化解密器
            Cipher cipher = Cipher.getInstance(AES_ALGORITHM);
            GCMParameterSpec parameterSpec = new GCMParameterSpec(GCM_TAG_LENGTH * 8, iv);
            cipher.init(Cipher.DECRYPT_MODE, secretKey, parameterSpec);

            // 解密
            byte[] plaintext = cipher.doFinal(ciphertext);
            return new String(plaintext, StandardCharsets.UTF_8);
        } catch (Exception e) {
            throw new RuntimeException("解密数据失败", e);
        }
    }

    public static String bytesToHex(byte[] bytes) {
        StringBuilder sb = new StringBuilder();
        for (byte b : bytes) {
            sb.append(String.format("%02X", b));
        }
        return sb.toString();
    }

    public static byte[] hexStringToByteArray(String hex) {
        int len = hex.length();
        byte[] data = new byte[len / 2];
        for (int i = 0; i < len; i += 2) {
            data[i / 2] = (byte) ((Character.digit(hex.charAt(i), 16) << 4)
                    + Character.digit(hex.charAt(i + 1), 16));
        }
        return data;
    }
}
