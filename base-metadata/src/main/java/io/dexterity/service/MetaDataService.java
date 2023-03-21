package io.dexterity.service;

import io.dexterity.client.MultipleEnv;
import io.dexterity.entity.MetaData;
import org.lmdbjava.Txn;
import org.springframework.stereotype.Service;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @author haoran
 * MataData的服务类，涉及到有关metadata的各种对外暴露的接口
 */
@Service
public interface MetaDataService {
    void insertNewMetadata(MetaData metaData, MultipleEnv env, Txn<ByteBuffer> parent);
    void  deleteMetadata(List<String> metadataKey,MultipleEnv multipleEnv,Txn<ByteBuffer> parent);
    void addNewMetadata(String key, MultipleEnv multipleEnv, Txn<ByteBuffer> parent, Map<String,String> additionalMD);

    Set<String> selectByMetaData(MetaData metaData,MultipleEnv multipleEnv,Txn<ByteBuffer> parent,String pageNumber,String pageSize);
    void insertPatch(List<MetaData> metaData, MultipleEnv multipleEnv, Txn<ByteBuffer> parent);
    Map<String,MetaData> selectMdByKeys(List<String> key,MultipleEnv multipleEnv,Txn<ByteBuffer> parent);
    Map<String, MetaData> selectMdByMdRange(String metadataKey, String lb,String ub, String prefix, MultipleEnv multipleEnv, Txn<ByteBuffer> parent,String pageNumber,String pageSize);
    Map<String, MetaData> selectMdByKeyRange(String lb,String ub, String prefix, MultipleEnv multipleEnv, Txn<ByteBuffer> parent,String pageNumber,String pageSize);
    Map<String, MetaData> selectMdByKeyPrefix(String prefix, MultipleEnv multipleEnv,Txn<ByteBuffer> parent,String pageNumber,String pageSize);
    boolean checkRetentionOrHold(MultipleEnv multipleEnv, Txn<ByteBuffer> parent,String key);



}
