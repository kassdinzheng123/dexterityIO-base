package io.dexterity.metadata.service.impl;

import cn.hutool.core.map.MapUtil;
import cn.hutool.core.util.HashUtil;
import io.dexterity.client.MultipleDBi;
import io.dexterity.client.MultipleEnv;
import io.dexterity.client.MultipleLmdb;
import io.dexterity.client.annotation.BucketName;
import io.dexterity.client.annotation.LmdbRead;
import io.dexterity.client.annotation.LmdbWrite;
import io.dexterity.client.aspect.LmdbTxn;
import io.dexterity.metadata.entity.MetaData;
import io.dexterity.metadata.entity.constants.MetaDataConstants;
import io.dexterity.metadata.service.MetaDataService;
import jakarta.annotation.Resource;
import lombok.extern.slf4j.Slf4j;
import org.lmdbjava.Meta;
import org.lmdbjava.Txn;
import org.springframework.lang.NonNull;
import org.springframework.stereotype.Service;

import java.lang.annotation.Target;
import java.nio.ByteBuffer;
import java.util.*;

/**
 * metaData基本服务
 */
@Service
@Slf4j
public class LmdbMetaDataService implements MetaDataService {

    /**
     * 往一个bucket所属的metadata数据库里面插入一条metadata
     * @param metaData 一个metadata对象，包含了数条基本metadata
     * @param bucketKey 定位bucket的key
     */

    @Resource
    private LmdbTxn lmdbTxn;


    /**
     * 插入一条新的元数据
     * 如果要插入多条，请使用patch
     * @param metaData 元数据对象
     * @param bucketKey 桶key
     */
    @Override
    @LmdbWrite
    public void insertNewMetadata(@NonNull MetaData metaData, @BucketName String bucketKey) {
        try{
            //首先做向前映射 即 key和value直接的对应
            MultipleEnv multipleEnv = lmdbTxn.getEnv().get();
            Txn<ByteBuffer> txn = lmdbTxn.getTxn().get();
            MultipleDBi multipleDBi = multipleEnv.buildDBInstance(MetaDataConstants.LMDB_METADATA_KEY, false, false);
            multipleDBi.putJsonObject(metaData.key, metaData.metaDataMap,txn);

            //然后做向后映射 value和key 进行映射方便查询
            for (var entry:metaData.metaDataMap.entrySet()) {
                MultipleDBi metaDBi = multipleEnv.buildDBInstance(entry.getKey(),false,true);
                //System.out.println(entry.getValue()+"->"+metaData.key);
                if (entry.getValue() != null) metaDBi.putAll(txn,MapUtil.of(entry.getValue(),Collections.singletonList(metaData.key)));
            }
        } catch (MultipleEnv.LMDBCreateFailedException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    @LmdbWrite
    public void insertPatch(List<MetaData> metaData, @BucketName String bucketKey) {
        try{
//            首先做向前映射 即 key和value直接的对应
            MultipleEnv multipleEnv = lmdbTxn.getEnv().get();
            Txn<ByteBuffer> txn = lmdbTxn.getTxn().get();

            MultipleDBi multipleDBi = multipleEnv.buildDBInstance(MetaDataConstants.LMDB_METADATA_KEY, false, false);
            Map<String,List<Map<String,String>>> putMap = new HashMap<>();
            metaData.forEach(
                    md-> {
                        putMap.putIfAbsent(md.key,new ArrayList<>());
                        putMap.get(md.key).add(md.metaDataMap);
                    }
            );
            multipleDBi.putAllJsonObject(txn,putMap);
            //数据库名称 -> 值 -> key
            Map<String,Map<String,List<String>>> metaDataPutMap = new HashMap<>();
            for (MetaData md:metaData){
                for (var entry:md.metaDataMap.entrySet()) {
                    metaDataPutMap.putIfAbsent(entry.getKey(),new HashMap<>());
                    Map<String, List<String>> values = metaDataPutMap.get(entry.getKey());
                    values.putIfAbsent(entry.getValue(),new ArrayList<>());
                    values.get(entry.getValue()).add(md.key);
                }
            }

            metaDataPutMap.forEach(
                    (key,value)->{
                        try {
                            MultipleDBi metaDBi = multipleEnv.buildDBInstance(key,false,true);
                            metaDBi.putAll(txn,value);
                        } catch (MultipleEnv.LMDBCreateFailedException e) {
                            throw new RuntimeException(e);
                        }
                    }
            );
//
//            txn.commit();
//            txn.close();
        } catch (MultipleEnv.LMDBCreateFailedException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    @LmdbRead
    public Map<String,MetaData> selectMetadata(List<String> key, @BucketName String bucketKey) {
        MultipleEnv multipleEnv = lmdbTxn.getEnv().get();
        Txn<ByteBuffer> txn = lmdbTxn.getTxn().get();
        try {
            MultipleDBi multipleDBi = multipleEnv.buildDBInstance(MetaDataConstants.LMDB_METADATA_KEY, false, false);
            Map<String, Map<String,String>> asObjects = multipleDBi.getAsObjects(txn, key);
            Map<String,MetaData> result = new HashMap<>();
            asObjects.forEach((k,v)->{
                MetaData metaData = new MetaData();
                metaData.key = k;
                metaData.metaDataMap.putAll(v);
                result.put(metaData.key, metaData);
            });
            return result;
        } catch (MultipleEnv.LMDBCreateFailedException e) {
            return null;
        }
    }

    /**
     * 按key删除metadata 双向映射都删
     * @param metadataKey metadata的key
     * @param bucketKey bucket
     */
    @Override
    @LmdbWrite
    public void deleteMetadata(String metadataKey,@BucketName String bucketKey) {
        try{
            //首先删向前映射 即 key和value直接的对应
            MultipleEnv multipleEnv = lmdbTxn.getEnv().get();
            Txn<ByteBuffer> txn = lmdbTxn.getTxn().get();
            MultipleDBi multipleDBi = multipleEnv.buildDBInstance(MetaDataConstants.LMDB_METADATA_KEY, false, false);
            Map<String,String> s = multipleDBi.getAsObject(metadataKey,txn);
            multipleDBi.delete(metadataKey);
            //然后删向后映射 value和key 进行映射方便查询
            for (var entry:s.entrySet()) {
                //打开对应的DB
                MultipleDBi metaDBi = multipleEnv.buildDBInstance(entry.getKey(),true,false);
                metaDBi.deleteFromDuplicatedData(entry.getValue(),metadataKey,txn);
            }
        } catch (MultipleEnv.LMDBCreateFailedException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 按照给定的metadata，查询符合条件的key
     * metadata中，只需要设置需要查询的元数据的值
     * 其他的元数据值保持为空即可
     * @param metaData
     * @param bucketKey
     * @return
     */
    @Override
    @LmdbRead
    public Set<String> selectByMetaData(MetaData metaData,@BucketName String bucketKey) {
        Set<String> keys = new HashSet<>();
        try{
            MultipleEnv multipleEnv = lmdbTxn.getEnv().get();
            Txn<ByteBuffer> txn = lmdbTxn.getTxn().get();
            for (var s:metaData.metaDataMap.entrySet()){
                String key = s.getKey();
                String value = s.getValue();
                //对每一个key开启一个数据库
                MultipleDBi multipleDBi =
                        multipleEnv.buildDBInstance(key, true, false);
                Set<String> probablyKeys = new HashSet<>(multipleDBi.getDuplicatedData(s.getValue(),txn));
                if (keys.isEmpty()){
                    keys.addAll(probablyKeys);
                }else{
                    keys.retainAll(probablyKeys);
                }
            }
            return keys;
        } catch (MultipleEnv.LMDBCreateFailedException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 对匹配matcher的元数据集合，增加一条 newMdValue
     * @param matcher 匹配用的metadata
     * @param newMdKey 新增加的metadata条目
     * @param newMdValue 新增加的metadata条目对应的值
     */
    @Override
    @LmdbWrite
    public void addNewMetadata(MetaData matcher,
                               @BucketName String bucketName,
                               String newMdKey,
                               String newMdValue) {
        try{
            MultipleEnv multipleEnv = MultipleLmdb.envs.get(bucketName);
            Txn<ByteBuffer> txn = lmdbTxn.getTxn().get();
            Set<String> strings = selectByMetaData(matcher,bucketName);
            MultipleDBi multipleDBi =
                    multipleEnv.buildDBInstance(MetaDataConstants.LMDB_METADATA_KEY, true, false);
            Map<String, Map<String,String>> metadataMap = multipleDBi.getAsObjects(strings);
            MultipleDBi metaDB =
                    multipleEnv.buildDBInstance(newMdKey, true, false);
            for (var key:metadataMap.keySet()){
                var map = metadataMap.get(key);
                map.put(newMdKey,newMdValue);
                multipleDBi.putJsonObject(key,map,txn);
                metaDB.put(newMdValue,key,txn);
            }
        } catch (MultipleEnv.LMDBCreateFailedException e) {
            throw new RuntimeException(e);
        }
    }


}
