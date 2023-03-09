package io.dexterity.metadata.entity;

import cn.hutool.core.lang.hash.Hash;
import io.dexterity.metadata.entity.constants.MetaDataConstants;
import lombok.Data;
import lombok.EqualsAndHashCode;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static io.dexterity.metadata.entity.constants.MetaDataConstants.*;
import static io.dexterity.metadata.entity.constants.MetaDataConstants.MIME;

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
        return metaDataMap.get(CREATED_UTC);
    }

    public void setCreateUTC(String createUTC) {
        metaDataMap.put(CREATED_UTC,createUTC);
    }

    public String getUpdateUTC() {
        return metaDataMap.get(UPDATE_UTC);
    }

    public void setUpdateUTC(String updateUTC) {
        metaDataMap.put(UPDATE_UTC,updateUTC);
    }

    public String getVersion() {
        return metaDataMap.get(VERSION);
    }

    public void setVersion(String version) {
        metaDataMap.put(VERSION,version);
    }

    public String getSize() {
        return metaDataMap.get("size");
    }

    public void setSize(String size) {
        metaDataMap.put("size",size);
    }

    public String getCheckSum() {
        return metaDataMap.get(CHECK_SUM);
    }

    public void setCheckSum(String checkSum) {
        metaDataMap.put(CHECK_SUM,checkSum);
    }

    public String getMIME(){
        return metaDataMap.get(MIME);
    }

    public void setMIME(String mime){
        metaDataMap.put(MIME,mime);
    }
}
