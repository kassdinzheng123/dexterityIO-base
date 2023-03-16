package io.dexterity.impl;

import io.dexterity.MetaDataApi;
import io.dexterity.client.MultipleEnv;
import io.dexterity.entity.MetaData;
import io.dexterity.service.MetaDataService;
import org.lmdbjava.Txn;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Service
public class MetaDataApiImpl implements MetaDataApi {
    @Autowired
    private MetaDataService metaDataService;

    @Override
    public void insertNewMetadata(MetaData metaData, MultipleEnv env, Txn<ByteBuffer> parent) {
        metaDataService.insertNewMetadata(metaData,env,parent);
    }

    @Override
    public void deleteMetadata(List<String> metadataKey, MultipleEnv multipleEnv, Txn<ByteBuffer> parent) {
        metaDataService.deleteMetadata(metadataKey,multipleEnv,parent);
    }

    @Override
    public void addNewMetadata(MetaData matcher, MultipleEnv multipleEnv, Txn<ByteBuffer> parent, String newMdKey, String newMdValue) {
        metaDataService.addNewMetadata(matcher,multipleEnv,parent,newMdKey,newMdValue);
    }

    @Override
    public Set<String> selectByMetaData(MetaData metaData, MultipleEnv multipleEnv, Txn<ByteBuffer> parent) {
        return metaDataService.selectByMetaData(metaData,multipleEnv,parent);
    }

    @Override
    public void insertPatch(List<MetaData> metaData, MultipleEnv multipleEnv, Txn<ByteBuffer> parent) {
        metaDataService.insertPatch(metaData,multipleEnv,parent);
    }

    @Override
    public Map<String, MetaData> selectMdByKeys(List<String> key, MultipleEnv multipleEnv, Txn<ByteBuffer> parent) {
        return metaDataService.selectMdByKeys(key,multipleEnv,parent);
    }

    @Override
    public Map<String, MetaData> selectMdByMdRange(String metadataKey, String lb, String ub, String prefix, MultipleEnv multipleEnv, Txn<ByteBuffer> parent) {
        return metaDataService.selectMdByMdRange(metadataKey,lb,ub,prefix,multipleEnv,parent);
    }

    @Override
    public Map<String, MetaData> selectMdByKeyRange(String lb, String ub, String prefix, MultipleEnv multipleEnv, Txn<ByteBuffer> parent) {
        return metaDataService.selectMdByKeyRange(lb,ub,prefix,multipleEnv,parent);
    }

    @Override
    public Map<String, MetaData> selectMdByKeyPrefix(String prefix, MultipleEnv multipleEnv, Txn<ByteBuffer> parent) {
        return metaDataService.selectMdByKeyPrefix(prefix,multipleEnv,parent);
    }
}
