package io.dexterity.entity;

import io.dexterity.entity.constants.MetaDataConstants;

import java.util.HashMap;
import java.util.Map;

public class MetaData {

    public String key;
    public final Map<String,String> metaDataMap;

    public MetaData(){
        metaDataMap = new HashMap<>();
    }

    private String getMetaData(String metaData){
        return metaDataMap.get(metaData);
    }

    private void putMetadata(String metadata,String val){
        metaDataMap.put(metadata,val);
    }

    public String getCreateUTC() {
        return metaDataMap.get(MetaDataConstants.CREATED_UTC);
    }

    public void setCreateUTC(String createUTC) {
        metaDataMap.put(MetaDataConstants.CREATED_UTC,createUTC);
    }

    public String getUpdateUTC() {
        return metaDataMap.get(MetaDataConstants.UPDATE_UTC);
    }

    public void setUpdateUTC(String updateUTC) {
        metaDataMap.put(MetaDataConstants.UPDATE_UTC,updateUTC);
    }

    public String getVersion() {
        return metaDataMap.get(MetaDataConstants.VERSION);
    }

    public void setVersion(String version) {
        metaDataMap.put(MetaDataConstants.VERSION,version);
    }

    public String getSize() {
        return metaDataMap.get("size");
    }

    public void setSize(String size) {
        metaDataMap.put("size",size);
    }

    public String getCheckSum() {
        return metaDataMap.get(MetaDataConstants.CHECK_SUM);
    }

    public void setCheckSum(String checkSum) {
        metaDataMap.put(MetaDataConstants.CHECK_SUM,checkSum);
    }

    public String getMIME(){
        return metaDataMap.get(MetaDataConstants.MIME);
    }

    public void setMIME(String mime){
        metaDataMap.put(MetaDataConstants.MIME,mime);
    }
}