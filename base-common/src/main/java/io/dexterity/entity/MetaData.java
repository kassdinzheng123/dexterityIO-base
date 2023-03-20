package io.dexterity.entity;

import java.util.HashMap;
import java.util.Map;

public class MetaData {
    public String key;
    public final Map<String,String> metaDataMap = new HashMap<>();

    public static final String CACHE_CONTROL = "Cache-Control";
    public static final String CONTENT_DISPOSITION = "Content-Disposition";
    public static final String CONTENT_ENCODING = "Content-Encoding";
    public static final String CONTENT_LANGUAGE = "Content-Language";
    public static final String CONTENT_LENGTH = "Content-Length";

    public static final String CONTENT_MD5 = "Content-MD5";
    public static final String CONTENT_TYPE = "Content-Type";
    public static final String EXPIRES = "Expires";
    public static final String LEGAL_HOLD = "Legal-Hold";

    public static final String RETENTION = "Retention";
    public static final String RETAIN_TIME = "RETAIN_TIME";

    public static final String DEXIO_USER = "DexIO-User-";

    private void setMetadata(String key, String value){
        this.metaDataMap.put(key,value);
    }

    public void setUserMetadata(String key, String value){
        if (key.startsWith(DEXIO_USER)){
            key = key.substring(key.indexOf(DEXIO_USER));
            this.metaDataMap.put(key,value);
        }else{
            throw new UserMDPrefixErrorException();
        }
    }

    static class UserMDPrefixErrorException extends RuntimeException{
        public UserMDPrefixErrorException(){
            super("user MD should be started with DexIO-User-,like DexIO-User-Name");
        }
    }

    public String getMetadata(String key){
        return this.metaDataMap.get(key);
    }

    public void removeMetadata(String key){
        this.metaDataMap.remove(key);
    }

    public boolean hasMetadata(String key){
        return this.metaDataMap.containsKey(key);
    }

    public void clearMetadata(){
        this.metaDataMap.clear();
    }

    public String getCacheControl() {
        return getMetadata(CACHE_CONTROL);
    }

    public void setCacheControl(String cacheControl) {
        setMetadata(CACHE_CONTROL,cacheControl);
    }

    public String getLegalHold() {
        return getMetadata(LEGAL_HOLD);
    }

    public String getContentLength() {
        return getMetadata(CONTENT_LENGTH);
    }

    public void setContentLength(String contentLength) {
        setMetadata(CONTENT_LENGTH,contentLength);
    }

    public void setLegalHold(String legalHold) {
        setMetadata(LEGAL_HOLD,legalHold);
    }

    public String getContentDisposition() {
        return getMetadata(CONTENT_DISPOSITION);
    }

    public void setContentDisposition(String contentDisposition) {
        setMetadata(CONTENT_DISPOSITION,contentDisposition);
    }

    public String getContentEncoding() {
        return getMetadata(CONTENT_ENCODING);
    }

    public void setContentEncoding(String contentEncoding) {
        setMetadata(CONTENT_ENCODING,contentEncoding);
    }

    public String getRetention() {
        return getMetadata(RETENTION);
    }

    public void setRetention(String retention) {
        setMetadata(RETENTION,retention);
    }

    public String getRetainTime() {
        return getMetadata(RETAIN_TIME);
    }

    public void setRetainTime(String retainTime) {
        setMetadata(RETAIN_TIME,retainTime);
    }

    public String getContentLanguage() {
        return getMetadata(CONTENT_LANGUAGE);
    }

    public void setContentLanguage(String contentLanguage) {
        setMetadata(CONTENT_LANGUAGE,contentLanguage);
    }

    public String getContentMD5() {
        return getMetadata(CONTENT_MD5);
    }

    public void setContentMD5(String contentMD5) {
        setMetadata(CONTENT_MD5,contentMD5);
    }

    public String getContentType() {
        return getMetadata(CONTENT_TYPE);
    }

    public void setContentType(String contentType) {
        setMetadata(CONTENT_TYPE,contentType);
    }

    public String getExpires() {
        return getMetadata(EXPIRES);
    }

    public void setExpires(String expires) {
        setMetadata(EXPIRES,expires);
    }

    @Override
    public String toString() {
        return "MetaData{" +
                "key='" + key + '\'' +
                ", metaDataMap=" + metaDataMap +
                '}';
    }
}
