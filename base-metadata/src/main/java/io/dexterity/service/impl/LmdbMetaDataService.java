package io.dexterity.service.impl;

import cn.hutool.core.map.MapUtil;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import io.dexterity.annotation.BucketName;
import io.dexterity.annotation.DupNames;
import io.dexterity.annotation.LmdbWrite;
import io.dexterity.annotation.UnDupNames;
import io.dexterity.aspect.LmdbTxn;
import io.dexterity.client.MultipleDBi;
import io.dexterity.client.MultipleEnv;
import io.dexterity.client.MultipleLmdb;
import io.dexterity.entity.BucketInfo;
import io.dexterity.entity.LMDBEnvSettings;
import io.dexterity.entity.LMDBEnvSettingsBuilder;
import io.dexterity.entity.MetaData;
import io.dexterity.entity.constants.MetaDataConstants;
import io.dexterity.service.MetaDataService;
import lombok.extern.slf4j.Slf4j;
import org.lmdbjava.Txn;
import org.springframework.stereotype.Service;

import java.lang.reflect.Type;
import java.nio.ByteBuffer;
import java.util.*;

/**
 * metaData基本服务
 */
@Service
@Slf4j
public class LmdbMetaDataService implements MetaDataService {


    private String fieldKey(String fieldName) {
        return "metaKAS-" + fieldName;
    }

    @LmdbWrite
    public void write(@BucketName String env,
                      @DupNames List<String> dupNames,
                      @UnDupNames List<String> unDupNames) throws Exception {
        MultipleEnv env1 = LmdbTxn.getEnv(env);
        Txn<ByteBuffer> writeTxn = LmdbTxn.getWriteTxn(env);
        MultipleDBi data = env1.getDB("data");
        for (int i = 0; i < 100000; i++) {
            data.put(UUID.randomUUID().toString(), "value", writeTxn);
        }
//        throw new Exception();
    }


    /**
     * 插入一条新的元数据
     * 如果要插入多条，请使用patch
     *
     * @param metaData 元数据对象
     */
    @Override
//    @LmdbWrite
    public void insertNewMetadata(MetaData metaData,
                                  MultipleEnv env,
                                  Txn<ByteBuffer> parent) {

        try (Txn<ByteBuffer> txn = env.getEnv().txn(parent)) {
            MultipleDBi keyDB = env.getDB(MetaDataConstants.LMDB_METADATA_KEY);
            keyDB.putJsonObject(metaData.key, metaData.metaDataMap, txn);
            //然后做向后映射 value和key 进行映射方便查询
            for (var entry : metaData.metaDataMap.entrySet()) {
                MultipleDBi metaDBi = env.getDB(entry.getKey());
                if (entry.getValue() != null && metaDBi!=null)
                    metaDBi.putAll(txn, MapUtil.of(entry.getValue(), Collections.singletonList(metaData.key)));
            }
            txn.commit();
        }

    }

    @Override
    public void insertPatch(List<MetaData> metaData, MultipleEnv multipleEnv, Txn<ByteBuffer> parent) {


        try (Txn<ByteBuffer> txn = multipleEnv.getEnv().txn(parent)) {


            Map<String, List<Map<String, String>>> putMap = new HashMap<>();
            metaData.forEach(
                    md -> {
                        putMap.putIfAbsent(md.key, new ArrayList<>());
                        putMap.get(md.key).add(md.metaDataMap);
                    }
            );

            MultipleDBi multipleDBi = multipleEnv.getDB(MetaDataConstants.LMDB_METADATA_KEY);
            multipleDBi.putAllJsonObject(txn, putMap);
            //数据库名称 -> 值 -> key
            Map<String, Map<String, List<String>>> metaDataPutMap = new HashMap<>();
            for (MetaData md : metaData) {
                for (var entry : md.metaDataMap.entrySet()) {
                    metaDataPutMap.putIfAbsent(entry.getKey(), new HashMap<>());
                    Map<String, List<String>> values = metaDataPutMap.get(entry.getKey());
                    values.putIfAbsent(entry.getValue(), new ArrayList<>());
                    values.get(entry.getValue()).add(md.key);
                }
            }

            metaDataPutMap.forEach(
                    (key, value) -> {
                        MultipleDBi metaDBi = multipleEnv.getDB(key);
                        metaDBi.putAll(txn, value);
                    }
            );

            //提交事务
            txn.commit();
        }

    }

    @Override
    public Map<String, MetaData> selectMdByKeys(List<String> key, MultipleEnv multipleEnv, Txn<ByteBuffer> parent) {
        MultipleDBi multipleDBi = multipleEnv.getDB(MetaDataConstants.LMDB_METADATA_KEY);
        if (key.isEmpty()) return MapUtil.empty();
        Map<String, Map<String, String>> asObjects = multipleDBi.getAsObjects(parent, key);
        Map<String, MetaData> result = new HashMap<>();
        asObjects.forEach((k, v) -> {
            MetaData metaData = new MetaData();
            metaData.key = k;
            if (v != null) {
                metaData.metaDataMap.putAll(v);
                result.put(metaData.key, metaData);
            }
        });
        return result;
    }

    /**
     * 根据某个metadata-key进行范围查找
     * 比如我要查找所有创建日期再时间戳100000到110000的key
     * 我只需要设置 lb 100000 ub 110000 即可！
     *
     * @param metadataKey metadata的key
     * @param lb          lb
     * @param ub          ub
     * @param multipleEnv 环境对象
     * @param parent      父事务
     * @return 返回mdKey-metadata的map形式
     */
    @Override
    public Map<String, MetaData> selectMdByMdRange(String metadataKey, String lb, String ub, String prefix,
                                                   MultipleEnv multipleEnv, Txn<ByteBuffer> parent,String pageNumber,String pageSize) {

        Map<String, MetaData> res = new HashMap<>();
        MultipleDBi db = multipleEnv.getDB(MetaDataConstants.LMDB_METADATA_KEY);
        MultipleDBi metaDB = multipleEnv.getDB(metadataKey);
        if (db == null || metaDB == null) return MapUtil.empty();
        List<Map.Entry<String, String>> ranged = metaDB.getRangedDuplicatedData(parent, lb, ub, prefix,pageNumber,pageSize);

        List<String> keyList = new ArrayList<>();
        ranged.forEach(
                entry -> {
                    keyList.add(entry.getValue());
                }
        );

        Map<String, Map<String, String>> patch = db.getAsObjects(parent, keyList);

        patch.forEach(
                (key, value) -> {
                    MetaData metaData = new MetaData();
                    metaData.metaDataMap.putAll(value);
                    metaData.key = key;
                    res.put(key, metaData);
                }
        );

        return res;
    }


    /**
     * 根据某个metadata的key进行范围查找
     * 比如我要查找所有具有前缀aaaa和bbbb之间的key  如 aaaa-key1 和 aaaa-key2 和 bbbb-key3 bbbb-key4
     * 我只需要设置 lb aaaa   ub bbbb 即可！
     *
     * @param lb 下界
     * @param ub 上界
     */
    @Override
    public Map<String, MetaData> selectMdByKeyRange(String lb, String ub, String prefix,
                                                    MultipleEnv multipleEnv, Txn<ByteBuffer> parent,String pageNumber,String pageSize) {
        Map<String, MetaData> res = new HashMap<>();
        MultipleDBi db = multipleEnv.getDB(MetaDataConstants.LMDB_METADATA_KEY);
        if (db == null) return MapUtil.empty();
        List<Map.Entry<String, String>> ranged = db.getRangedDuplicatedData(parent, lb, ub, prefix,pageNumber,pageSize);

        Type type = new TypeToken<Map<String, String>>() {
        }.getType();
        Gson gson = new Gson();
        ranged.forEach(
                entry -> {
                    MetaData metaData = new MetaData();
                    metaData.metaDataMap.putAll(gson.fromJson(entry.getValue(), type));
                    metaData.key = entry.getKey();
                    res.put(entry.getKey(), metaData);
                });

        return res;
    }

    /**
     * 按照前缀查找元数据
     * 比如 所有以aaaa-开头的元数据 和range查找一样，都可以用于文件夹的构建
     *
     * @param prefix 前缀
     * @return
     */
    @Override
    public Map<String, MetaData> selectMdByKeyPrefix(String prefix, MultipleEnv multipleEnv, Txn<ByteBuffer> parent,String pageNumber,String pageSize) {
        Map<String, MetaData> res = new HashMap<>();
        MultipleDBi db = multipleEnv.getDB(MetaDataConstants.LMDB_METADATA_KEY);
        if (db == null) return MapUtil.empty();
        List<Map.Entry<String, String>> ranged = db.prefixSearch(parent, prefix,pageNumber,pageSize);

        Type type = new TypeToken<Map<String, String>>() {
        }.getType();
        Gson gson = new Gson();
        ranged.forEach(
                entry -> {
                    MetaData metaData = new MetaData();
                    metaData.metaDataMap.putAll(gson.fromJson(entry.getValue(), type));
                    metaData.key = entry.getKey();
                    res.put(entry.getKey(), metaData);
                });

        return res;
    }

    /**
     * 检查对象是否属于保护状态
     * @param key key
     * @return 是否属于被保护的对象
     */
    @Override
    public boolean checkRetentionOrHold(MultipleEnv multipleEnv, Txn<ByteBuffer> parent,String key) {
        Map<String, MetaData> stringMetaDataMap = selectMdByKeys(List.of(key), multipleEnv, parent);
        MetaData metaData = stringMetaDataMap.get(key);
        return  (Boolean.parseBoolean(metaData.getRetention()) && System.currentTimeMillis() <= Long.parseLong(metaData.getRetainTime()))
            && Boolean.parseBoolean(metaData.getLegalHold());
    }

    @Override
    public void insertBucketInfo(BucketInfo bucketInfo,MultipleEnv multipleEnv, Txn<ByteBuffer> parent) {
        //第一步 buildEnv
        String metadataLimit = bucketInfo.getMetadataLimit();
        String maxReader = bucketInfo.getMaxReader();

        LMDBEnvSettings build = LMDBEnvSettingsBuilder.startBuild().maxSize(1024 * 1024 * 100L)
                .maxDBInstance(200)
                .maxDBInstance(Integer.parseInt(metadataLimit))
                .maxReaders(Integer.parseInt(maxReader)).filePosition("D://").build();
        MultipleEnv multipleEnv1 = MultipleLmdb.buildNewEnv(build);

        try (Txn<ByteBuffer> txn = multipleEnv.getEnv().txn(parent)) {

        }
    }

    /**
     * 按key删除metadata 双向映射都删
     *
     * @param metadataKey metadata的key
     */
    @Override
    public void deleteMetadata(List<String> metadataKey, MultipleEnv multipleEnv, Txn<ByteBuffer> parent) {


        try (Txn<ByteBuffer> txn = multipleEnv.getEnv().txn(parent)) {
            //首先删向前映射 即 key和value直接的对应
            MultipleDBi multipleDBi = multipleEnv.getDB(MetaDataConstants.LMDB_METADATA_KEY);
            multipleDBi.deletePatch(metadataKey, txn);
            Map<String, Map<String, String>> s = multipleDBi.getAsObjects(txn, metadataKey);
            //然后删向后映射 value和key 进行映射方便查询
            for (var entry : s.entrySet()) {
                //打开对应的DB
                Map<String, String> value = entry.getValue();
                if (value != null) {
                    for (var metadataKV : value.entrySet()) {
                        MultipleDBi metaDBi = multipleEnv.getDB(metadataKV.getKey());
                        metaDBi.deleteFromDuplicatedData(metadataKV.getValue(), metadataKV.getKey(), txn);
                    }
                }
            }
            txn.commit();
        }
    }

    /**
     * 按照给定的metadata，查询符合条件的key
     * metadata中，只需要设置需要查询的元数据的值
     * 其他的元数据值保持为空即可
     *
     * @param metaData
     * @return
     */
    @Override
    public Set<String> selectByMetaData(MetaData metaData, MultipleEnv multipleEnv, Txn<ByteBuffer> parent,String pageNumber,String pageSize) {
        Set<String> keys = new HashSet<>();
        for (var s : metaData.metaDataMap.entrySet()) {
            String key = s.getKey();
            //对每一个key开启一个数据库
            MultipleDBi multipleDBi = multipleEnv.getDB(key);
            Set<String> probablyKeys = new HashSet<>(multipleDBi.getDuplicatedData(s.getValue(), parent));
            if (keys.isEmpty()) {
                keys.addAll(probablyKeys);
            } else {
                keys.retainAll(probablyKeys);
            }
        }
        return keys;


    }

    @Override
    public void addNewMetadata(String key, MultipleEnv multipleEnv, Txn<ByteBuffer> parent, Map<String,String> additionalMD) {

        try (Txn<ByteBuffer> txn = multipleEnv.getEnv().txn(parent)) {
            MultipleDBi multipleDBi = multipleEnv.getDB(MetaDataConstants.LMDB_METADATA_KEY);
            Map<String, Map<String, String>> metadataMap = multipleDBi.getAsObjects(txn,key);
            var map = metadataMap.get(key);
            map.putAll(additionalMD);
            multipleDBi.putJsonObject(key, map, txn);
            additionalMD.forEach(
                    (k,v)->{
                        MultipleDBi metaDB = multipleEnv.getDB(k);
                        metaDB.put(v, key, txn);
                    }
            );
            txn.commit();
        }
    }


}
