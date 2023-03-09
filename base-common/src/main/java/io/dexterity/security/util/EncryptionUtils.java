package io.dexterity.security.util;

import org.jasypt.encryption.pbe.StandardPBEStringEncryptor;
import org.jasypt.salt.StringFixedSaltGenerator;
import org.jasypt.salt.ZeroSaltGenerator;

public class EncryptionUtils {
    private final StandardPBEStringEncryptor encryptor;

    public EncryptionUtils(String password){
        StandardPBEStringEncryptor encryptor = new StandardPBEStringEncryptor();
        encryptor.setPassword(password); // 设置加密密钥
        encryptor.setSaltGenerator(new StringFixedSaltGenerator("akjuiheihqw"));
        encryptor.setAlgorithm("PBEWithMD5AndTripleDES"); // 设置加密算法
        this.encryptor = encryptor;
    }

    public String encrypt(String plainText) {
        return encryptor.encrypt(plainText);
    }

    public String decrypt(String cipherText) {
        return encryptor.decrypt(cipherText);
    }

}