package io.dexterity;

import io.dexterity.client.MultipleEnv;
import io.dexterity.entity.MetaData;
import org.lmdbjava.Txn;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Set;

public interface MetaDataApi {
    void insertNewMetadata(MetaData metaData, MultipleEnv env, Txn<ByteBuffer> parent);
    void insertPatch(List<MetaData> metaData,  MultipleEnv multipleEnv,Txn<ByteBuffer> parent);
    void deleteMetadata(List<String> metadataKey, MultipleEnv multipleEnv, Txn<ByteBuffer> parent);
    void addNewMetadata(MetaData matcher, MultipleEnv multipleEnv, Txn<ByteBuffer> parent, String newMdKey, String newMdValue);
    Set<String> selectByMetaData(MetaData metaData, MultipleEnv multipleEnv, Txn<ByteBuffer> parent);
    Map<String,MetaData> selectMdByKeys(List<String> key, MultipleEnv multipleEnv, Txn<ByteBuffer> parent);
    Map<String, MetaData> selectMdByMdRange(String metadataKey, String lb,String ub, String prefix, MultipleEnv multipleEnv, Txn<ByteBuffer> parent);
    Map<String, MetaData> selectMdByKeyRange(String lb,String ub, String prefix, MultipleEnv multipleEnv, Txn<ByteBuffer> parent);
    Map<String, MetaData> selectMdByKeyPrefix(String prefix, MultipleEnv multipleEnv,Txn<ByteBuffer> parent);
}
