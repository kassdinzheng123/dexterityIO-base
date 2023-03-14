package io.dexterity.service.impl;

import cn.hutool.core.map.MapUtil;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import io.dexterity.annotation.BucketName;
import io.dexterity.client.MultipleDBi;
import io.dexterity.client.MultipleEnv;
import io.dexterity.client.MultipleLmdb;
import io.dexterity.entity.MetaData;
import io.dexterity.entity.constants.MetaDataConstants;
import io.dexterity.service.MetaDataService;
import lombok.extern.slf4j.Slf4j;
import org.lmdbjava.Env;
import org.lmdbjava.Txn;
import org.springframework.lang.NonNull;
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


    /**
     * 插入一条新的元数据
     * 如果要插入多条，请使用patch
     * @param metaData 元数据对象
     * @param bucketKey 桶key
     */
    @Override
//    @LmdbWrite
    public void insertNewMetadata(@NonNull MetaData metaData, @BucketName String bucketKey) {
        try{
            //首先做向前映射 即 key和value直接的对应
            MultipleEnv multipleEnv = MultipleLmdb.envs.get(bucketKey);

            multipleEnv.buildDBInstance(MetaDataConstants.LMDB_METADATA_KEY, false, false);

            for (var entry:metaData.metaDataMap.entrySet()) {
                multipleEnv.buildDBInstance(entry.getKey(),false,true);
            }


            MultipleLmdb.checkAndExpand(bucketKey,metaData);
            Env<ByteBuffer> env = multipleEnv.getEnv();

            try (Txn<ByteBuffer> txn = env.txnWrite()) {
                //            Txn<ByteBuffer> txn = lmdbTxn.getTxn().get();
                MultipleDBi keyDB = multipleEnv.buildDBInstance(MetaDataConstants.LMDB_METADATA_KEY, false, false);
                keyDB.putJsonObject(metaData.key, metaData.metaDataMap,txn);
                System.out.println(txn.getId());
                //然后做向后映射 value和key 进行映射方便查询
                for (var entry:metaData.metaDataMap.entrySet()) {
                    MultipleDBi metaDBi = multipleEnv.getLmdbMaps().get(entry.getKey());
                    if (entry.getValue() != null) metaDBi.putAll(txn,MapUtil.of(entry.getValue(),Collections.singletonList(metaData.key)));
                }
                txn.commit();
            }
        } catch (MultipleEnv.LMDBCreateFailedException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void insertPatch(List<MetaData> metaData, @BucketName String bucketKey) {

//            首先做向前映射 即 key和value直接的对应
            MultipleEnv multipleEnv =  MultipleLmdb.envs.get(bucketKey);

            //提前初始化好数据库
        try {
            multipleEnv.buildDBInstance(MetaDataConstants.LMDB_METADATA_KEY, false, false);

            //其实还好 这里的时间花费
            for (MetaData md:metaData){
                for (var entry:md.metaDataMap.entrySet()) {
                    multipleEnv.buildDBInstance(entry.getKey(),false,true);
                }
            }

        } catch (MultipleEnv.LMDBCreateFailedException e) {
            throw new RuntimeException(e);
        }


        Txn<ByteBuffer> txn = multipleEnv.getEnv().txnWrite();
        try{

            Map<String,List<Map<String,String>>> putMap = new HashMap<>();
            metaData.forEach(
                    md-> {
                        putMap.putIfAbsent(md.key,new ArrayList<>());
                        putMap.get(md.key).add(md.metaDataMap);
                    }
            );

            MultipleDBi multipleDBi = multipleEnv.getDB(MetaDataConstants.LMDB_METADATA_KEY);
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
                        MultipleDBi metaDBi = multipleEnv.getDB(key);
                        metaDBi.putAll(txn,value);
                    }
            );

            //提交事务
            txn.commit();
            txn.close();
        } catch (Env.MapFullException mf){
            txn.abort();
            txn.close();
            MultipleLmdb.checkAndExpand(bucketKey,metaData);
            insertPatch(metaData,bucketKey);
        }
    }

    @Override
    public Map<String,MetaData> selectMdByKeys(List<String> key, @BucketName String bucketKey) {
        MultipleEnv multipleEnv = MultipleLmdb.envs.get(bucketKey);
        try (Txn<ByteBuffer> txn = multipleEnv.getEnv().txnRead()) {
            MultipleDBi multipleDBi = multipleEnv.buildDBInstance(MetaDataConstants.LMDB_METADATA_KEY, false, false);
            Map<String, Map<String, String>> asObjects = multipleDBi.getAsObjects(txn, key);
            Map<String, MetaData> result = new HashMap<>();
            asObjects.forEach((k, v) -> {
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
     * 根据某个metadata-key进行范围查找
     * 比如我要查找所有创建日期再时间戳100000到110000的key
     * 我只需要设置 lb 100000 ub 110000 即可！
     * @param metadataKey metadata的key
     * @param lb lb
     * @param ub ub
     * @param bucketKey 存储桶
     * @return 返回mdKey-metadata的map形式
     */
    @Override
    public Map<String, MetaData> selectMdByMdRange(String metadataKey, String lb, String ub, String bucketKey) {

        Map<String, MetaData> res = new HashMap<>();
        MultipleEnv multipleEnv = MultipleLmdb.envs.get(bucketKey);
        MultipleDBi db = multipleEnv.getDB(MetaDataConstants.LMDB_METADATA_KEY);
        MultipleDBi metaDB = multipleEnv.getDB(metadataKey);
        if (db == null || metaDB == null) return MapUtil.empty();
        try (Txn<ByteBuffer> txn = multipleEnv.getEnv().txnRead()) {
            List<Map.Entry<String, String>> ranged = metaDB.getRangedDuplicatedData(txn, lb, ub);

            List<String> keyList = new ArrayList<>();
            ranged.forEach(
                    entry->{
                        keyList.add(entry.getValue());
                    }
            );

            Map<String, Map<String,String>> patch = db.getAsObjects(txn,keyList);

            patch.forEach(
                    (key,value)->{
                        MetaData metaData = new MetaData();
                        metaData.metaDataMap.putAll(value);
                        metaData.key = key;
                        res.put(key,metaData);
                    }
            );
        }
        return res;
    }

    /**
     * 根据某个metadata的key进行范围查找
     * 比如我要查找所有具有前缀aaaa和bbbb之间的key  如 aaaa-key1 和 aaaa-key2 和 bbbb-key3 bbbb-key4
     * 我只需要设置 lb aaaa   ub bbbb 即可！
     * @param lb 下界
     * @param ub 上界
     * @param bucketKey bucketKey
     * @return
     */
    @Override
    public Map<String, MetaData> selectMdByKeyRange(String lb, String ub, String bucketKey) {
        Map<String, MetaData> res = new HashMap<>();
        MultipleEnv multipleEnv = MultipleLmdb.envs.get(bucketKey);
        MultipleDBi db = multipleEnv.getDB(MetaDataConstants.LMDB_METADATA_KEY);
        if (db == null ) return MapUtil.empty();
        try (Txn<ByteBuffer> txn = multipleEnv.getEnv().txnRead()) {
            List<Map.Entry<String, String>> ranged = db.getRangedDuplicatedData(txn, lb, ub);

            Type type = new TypeToken<Map<String,String>>(){}.getType();
            Gson gson = new Gson();
            ranged.forEach(
                    entry->{
                        MetaData metaData = new MetaData();
                        metaData.metaDataMap.putAll(gson.fromJson(entry.getValue(),type));
                        metaData.key = entry.getKey();
                        res.put(entry.getKey(),metaData);
                    }
            );
        }
        return res;
    }

    /**
     * 按照前缀查找元数据
     * 比如 所有以aaaa-开头的元数据 和range查找一样，都可以用于文件夹的构建
     * @param prefix 前缀
     * @param bucketKey 存储桶的key
     * @return
     */
    @Override
    public Map<String, MetaData> selectMdByKeyPrefix(String prefix, String bucketKey) {
        Map<String, MetaData> res = new HashMap<>();
        MultipleEnv multipleEnv = MultipleLmdb.envs.get(bucketKey);
        MultipleDBi db = multipleEnv.getDB(MetaDataConstants.LMDB_METADATA_KEY);
        if (db == null ) return MapUtil.empty();
        try (Txn<ByteBuffer> txn = multipleEnv.getEnv().txnRead()) {
            List<Map.Entry<String, String>> ranged = db.prefixSearch(txn,prefix);

            Type type = new TypeToken<Map<String,String>>(){}.getType();
            Gson gson = new Gson();
            ranged.forEach(
                    entry->{
                        MetaData metaData = new MetaData();
                        metaData.metaDataMap.putAll(gson.fromJson(entry.getValue(),type));
                        metaData.key = entry.getKey();
                        res.put(entry.getKey(),metaData);
                    }
            );
        }
        return res;
    }

    /**
     * 按key删除metadata 双向映射都删
     * @param metadataKey metadata的key
     * @param bucketKey bucket
     */
    @Override
    public void deleteMetadata(List<String> metadataKey,@BucketName String bucketKey) {

        MultipleEnv multipleEnv = MultipleLmdb.envs.get(bucketKey);

        MultipleDBi multipleDBi = null;
        Map<String,Map<String,String>> s = new HashMap<>();
        try {
            multipleDBi = multipleEnv.buildDBInstance(MetaDataConstants.LMDB_METADATA_KEY, false, false);
            s = multipleDBi.getAsObjects(metadataKey);

            for (var entry:s.entrySet()) {
                for (var metadataKV : entry.getValue().entrySet()) {
                    multipleEnv.buildDBInstance(metadataKV.getKey(), true, false);
                }
            }

        } catch (MultipleEnv.LMDBCreateFailedException e) {
            System.out.println(multipleEnv.getEnv().getDbiNames().size());
            throw new RuntimeException(e);
        }


        try (Txn<ByteBuffer> txn = multipleEnv.getEnv().txnWrite()) {
            //首先删向前映射 即 key和value直接的对应
            multipleDBi = multipleEnv.getDB(MetaDataConstants.LMDB_METADATA_KEY);
            multipleDBi.deletePatch(metadataKey,txn);
            //然后删向后映射 value和key 进行映射方便查询
            for (var entry : s.entrySet()) {
                //打开对应的DB
                for (var metadataKV : entry.getValue().entrySet()){
                    MultipleDBi metaDBi = multipleEnv.getDB(metadataKV.getKey());
                    metaDBi.deleteFromDuplicatedData(metadataKV.getValue(), metadataKV.getKey(), txn);
                }
            }
            txn.commit();
        } finally {
            MultipleLmdb.checkAndReduce(bucketKey);
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
    public Set<String> selectByMetaData(MetaData metaData,@BucketName String bucketKey) {
        Set<String> keys = new HashSet<>();
        MultipleEnv multipleEnv = MultipleLmdb.envs.get(bucketKey);
        try (Txn<ByteBuffer> txn = multipleEnv.getEnv().txnRead()) {
            for (var s : metaData.metaDataMap.entrySet()) {
                String key = s.getKey();
                String value = s.getValue();
                //对每一个key开启一个数据库
                MultipleDBi multipleDBi =
                        multipleEnv.buildDBInstance(key, true, false);
                Set<String> probablyKeys = new HashSet<>(multipleDBi.getDuplicatedData(s.getValue(), txn));
                if (keys.isEmpty()) {
                    keys.addAll(probablyKeys);
                } else {
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
    public void addNewMetadata(MetaData matcher,
                               @BucketName String bucketName,
                               String newMdKey,
                               String newMdValue) {
        MultipleEnv multipleEnv = MultipleLmdb.envs.get(bucketName);

        MultipleDBi multipleDBi = null;
        try {
            multipleDBi = multipleEnv.buildDBInstance(MetaDataConstants.LMDB_METADATA_KEY, true, false);
            multipleEnv.buildDBInstance(newMdKey, true, false);
        } catch (MultipleEnv.LMDBCreateFailedException e) {
            throw new RuntimeException(e);
        }


        try (Txn<ByteBuffer> txn = multipleEnv.getEnv().txnWrite()) {
            Set<String> strings = selectByMetaData(matcher, bucketName);
            Map<String, Map<String, String>> metadataMap = multipleDBi.getAsObjects(strings);
            MultipleDBi metaDB = multipleEnv.getDB(newMdKey);
            for (var key : metadataMap.keySet()) {
                var map = metadataMap.get(key);
                map.put(newMdKey, newMdValue);
                multipleDBi.putJsonObject(key, map, txn);
                metaDB.put(newMdValue, key, txn);
            }
            txn.commit();
        }
    }


}
