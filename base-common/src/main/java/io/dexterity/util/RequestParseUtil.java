package io.dexterity.util;

import io.dexterity.entity.header.DexIOHost;
import io.dexterity.entity.header.PublicRequestHeader;
import io.dexterity.exception.UnexpectedRequestBodyException;
import jakarta.servlet.http.HttpServletRequest;
import lombok.extern.slf4j.Slf4j;

import java.io.InputStream;
import java.security.MessageDigest;

@Slf4j
public class RequestParseUtil {

    public static DexIOHost parseS3Host(String host) {
        String[] parts = host.split("\\.");
        if (parts.length < 4) {
            throw new IllegalArgumentException("Invalid DexIO host: " + host);
        }
        String region = parts[1];
        String bucketName = parts[0];
        StringBuilder keyBuilder = new StringBuilder();
        for (int i = 2; i < parts.length; i++) {
            keyBuilder.append(parts[i]);
            if (i < parts.length - 1) {
                keyBuilder.append(".");
            }
        }
        String key = keyBuilder.toString();
        return new DexIOHost(region,bucketName,key);
    }

    public static boolean verifyRequestBody(HttpServletRequest request, String expectedMd5) {
        try{
            InputStream inputStream = request.getInputStream();
            MessageDigest md = MessageDigest.getInstance("MD5");
            byte[] buffer = new byte[8192];
            int read = 0;
            while ((read = inputStream.read(buffer)) > 0) {
                md.update(buffer, 0, read);
            }
            byte[] md5Bytes = md.digest();
            StringBuilder sb = new StringBuilder();
            for (byte b : md5Bytes) {
                sb.append(String.format("%02x", b));
            }
            String actualMd5 = sb.toString();
            return actualMd5.equals(expectedMd5);
        }catch (Exception e){
            log.info("MD5 Request body Check failed");
            e.printStackTrace();
            return false;
        }
    }

    public static PublicRequestHeader readFromHttpHeaders(HttpServletRequest request) throws UnexpectedRequestBodyException {
        PublicRequestHeader publicRequestHeader = new PublicRequestHeader();
        publicRequestHeader.setAuthorization(request.getHeader("Authorization"));
        publicRequestHeader.setContentLength(request.getIntHeader("Content-Length"));
        publicRequestHeader.setContentType(request.getHeader("Content-Type"));
        publicRequestHeader.setContentMD5(request.getHeader("Content-MD5"));
        boolean b = verifyRequestBody(request, publicRequestHeader.getContentMD5());
        if (!b){
            throw new UnexpectedRequestBodyException();
        }
        publicRequestHeader.setDate(request.getHeader("Date"));
        publicRequestHeader.setHost(request.getHeader("Host"));
        return publicRequestHeader;
    }


}
