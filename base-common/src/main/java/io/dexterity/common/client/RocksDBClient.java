package io.dexterity.common.client;

import lombok.extern.slf4j.Slf4j;
import org.rocksdb.*;
import org.springframework.util.ObjectUtils;

import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@Slf4j
public class RocksDBClient {
    private static RocksDB rocksDB;
    public static ConcurrentMap<String, ColumnFamilyHandle> columnFamilyHandleMap = new ConcurrentHashMap<>(); //数据库列族(表)集合
    public static int GET_KEYS_BATCH_SIZE = 100000;
    private RocksDBClient() {
    }
    /*
      初始化 RocksDB
     */
    static{
        try{
            String osName = System.getProperty("os.name"); // 获取当前操作系统:Windows 11
            log.info("osName:{}", osName);
            String rocksDBPath; // RocksDB文件目录
            if (osName.toLowerCase().contains("windows")) {
                rocksDBPath = "E:\\RocksDB"; // 指定windows系统下RocksDB文件目录
            } else {
                rocksDBPath = "/usr/local/rocksdb"; // 指定linux系统下RocksDB文件目录
            }
            RocksDB.loadLibrary(); // 加载RocksDB c++库的静态方法
            Options options = new Options(); // Options类包含一组可配置的DB选项,决定数据库的行为
            options.setCreateIfMissing(true); // 如果数据库不存在则创建
            List<byte[]> cfArr = RocksDB.listColumnFamilies(options, rocksDBPath); // 初始化所有已存在列族
            List<ColumnFamilyDescriptor> columnFamilyDescriptors = new ArrayList<>(); // ColumnFamilyDescriptor集合
            if (!ObjectUtils.isEmpty(cfArr)) {
                for (byte[] cf : cfArr) {
                    columnFamilyDescriptors.add(new ColumnFamilyDescriptor(cf, new ColumnFamilyOptions()));
                }
            } else {
                columnFamilyDescriptors.add(new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, new ColumnFamilyOptions()));
            }
            DBOptions dbOptions = new DBOptions();
            dbOptions.setCreateIfMissing(true);
            List<ColumnFamilyHandle> columnFamilyHandles = new ArrayList<>(); //ColumnFamilyHandle集合
            // 返回一个RocksDB实例的工厂方法
            rocksDB = RocksDB.open(dbOptions, rocksDBPath, columnFamilyDescriptors, columnFamilyHandles);
            for (int i = 0; i < columnFamilyDescriptors.size(); i++) {
                ColumnFamilyHandle columnFamilyHandle = columnFamilyHandles.get(i);
                String cfName = new String(columnFamilyDescriptors.get(i).getName(), StandardCharsets.UTF_8);
                columnFamilyHandleMap.put(cfName, columnFamilyHandle);
            }
            log.info("RocksDB init success!! path:{}", rocksDBPath);
            log.info("cfNames:{}", columnFamilyHandleMap.keySet());
        }catch (Exception e){
            log.error("RocksDB init failure!! error:{}", e.getMessage());
            e.printStackTrace();
        }
    }

    /**
     * 列族，创建（如果不存在）
     */
    public static ColumnFamilyHandle cfAddIfNotExist(String cfName) throws RocksDBException {
        ColumnFamilyHandle columnFamilyHandle;
        if (!columnFamilyHandleMap.containsKey(cfName)) {
            columnFamilyHandle = rocksDB.createColumnFamily(new ColumnFamilyDescriptor(cfName.getBytes(), new ColumnFamilyOptions()));
            columnFamilyHandleMap.put(cfName, columnFamilyHandle);
            log.info("cfAddIfNotExist success!! cfName:{}", cfName);
        } else {
            columnFamilyHandle = columnFamilyHandleMap.get(cfName);
        }
        return columnFamilyHandle;
    }

    /**
     * 列族，删除（如果存在）
     */
    public static void cfDeleteIfExist(String cfName) throws RocksDBException {
        if (columnFamilyHandleMap.containsKey(cfName)) {
            rocksDB.dropColumnFamily(columnFamilyHandleMap.get(cfName));
            columnFamilyHandleMap.remove(cfName);
            log.info("cfDeleteIfExist success!! cfName:{}", cfName);
        } else {
            log.warn("cfDeleteIfExist containsKey!! cfName:{}", cfName);
        }
    }

    /**
     * 增
     */
    public static void put(String cfName, byte[] key, byte[] value) throws RocksDBException {
        ColumnFamilyHandle columnFamilyHandle = cfAddIfNotExist(cfName); //获取列族Handle
        rocksDB.put(columnFamilyHandle, key, value);
    }

    /**
     * 增（批量）
     */
    public static void batchPut(String cfName, Map<byte[], byte[]> map) throws RocksDBException {
        ColumnFamilyHandle columnFamilyHandle = cfAddIfNotExist(cfName); //获取列族Handle
        WriteOptions writeOptions = new WriteOptions();
        WriteBatch writeBatch = new WriteBatch();
        for (Map.Entry<byte[], byte[]> entry : map.entrySet()) {
            writeBatch.put(columnFamilyHandle, entry.getKey(), entry.getValue());
        }
        rocksDB.write(writeOptions, writeBatch);
    }

    /**
     * 根据key删kv
     */
    public static void delete(String cfName, byte[] key) throws RocksDBException {
        ColumnFamilyHandle columnFamilyHandle = cfAddIfNotExist(cfName); //获取列族Handle
        rocksDB.delete(columnFamilyHandle, key);
    }

    /**
     * 根据key查value
     */
    public static byte[] get(String cfName, byte[] key) throws RocksDBException {
        ColumnFamilyHandle columnFamilyHandle = cfAddIfNotExist(cfName); //获取列族Handle
        return rocksDB.get(columnFamilyHandle, key);
    }

    /**
     * 根据多个key，查多个KV
     */
    public static Map<byte[], byte[]> multiGetAsMap(String cfName, List<byte[]> keys) throws RocksDBException {
        Map<byte[], byte[]> map = new HashMap<>(keys.size());
        ColumnFamilyHandle columnFamilyHandle = cfAddIfNotExist(cfName); //获取列族Handle
        List<ColumnFamilyHandle> columnFamilyHandles;
        columnFamilyHandles = IntStream.range(0, keys.size()).mapToObj(i -> columnFamilyHandle).collect(Collectors.toList());
        List<byte[]> bytes = rocksDB.multiGetAsList(columnFamilyHandles, keys);
        for (int i = 0; i < bytes.size(); i++) {
            map.put(keys.get(i), bytes.get(i));
        }
        return map;
    }

    /**
     * 根据多个key，查多个value
     */
    public static List<byte[]> multiGetValueAsList(String cfName, List<byte[]> keys) throws RocksDBException {
        ColumnFamilyHandle columnFamilyHandle = cfAddIfNotExist(cfName); //获取列族Handle
        List<ColumnFamilyHandle> columnFamilyHandles = new ArrayList<>();
        for (int i = 0; i < keys.size(); i++) {
            columnFamilyHandles.add(columnFamilyHandle);
        }
        return rocksDB.multiGetAsList(columnFamilyHandles, keys);
    }

    /**
     * 根据列族查所有key
     */
    public static List<byte[]> getAllKey(String cfName) throws RocksDBException {
        List<byte[]> list = new ArrayList<>();
        ColumnFamilyHandle columnFamilyHandle = cfAddIfNotExist(cfName); //获取列族Handle
        try (RocksIterator rocksIterator = rocksDB.newIterator(columnFamilyHandle)) {
            for (rocksIterator.seekToFirst(); rocksIterator.isValid(); rocksIterator.next()) {
                list.add(rocksIterator.key());
            }
        }
        return list;
    }

    /**
     * 根据列族，游标查所有key，从lastKey开始查
     */
    public static List<byte[]> getKeysFrom(String cfName, byte[] lastKey) throws RocksDBException {
        List<byte[]> list = new ArrayList<>(GET_KEYS_BATCH_SIZE);
        // 获取列族Handle
        ColumnFamilyHandle columnFamilyHandle = cfAddIfNotExist(cfName);
        try (RocksIterator rocksIterator = rocksDB.newIterator(columnFamilyHandle)) {
            if (lastKey != null) {
                rocksIterator.seek(lastKey);
                rocksIterator.next();
            } else {
                rocksIterator.seekToFirst();
            }
            // 一批次最多 GET_KEYS_BATCH_SIZE 个 key
            while (rocksIterator.isValid() && list.size() < GET_KEYS_BATCH_SIZE) {
                list.add(rocksIterator.key());
                rocksIterator.next();
            }
        }
        return list;
    }

    /**
     * 根据列族查全部KV
     */
    public static Map<byte[], byte[]> getAll(String cfName) throws RocksDBException {
        Map<byte[], byte[]> map = new HashMap<>();
        ColumnFamilyHandle columnFamilyHandle = cfAddIfNotExist(cfName); //获取列族Handle
        try (RocksIterator rocksIterator = rocksDB.newIterator(columnFamilyHandle)) {
            for (rocksIterator.seekToFirst(); rocksIterator.isValid(); rocksIterator.next()) {
                map.put(rocksIterator.key(), rocksIterator.value());
            }
        }
        return map;
    }

    /**
     * 查总条数
     */
    public static int getCount(String cfName) throws RocksDBException {
        int count = 0;
        ColumnFamilyHandle columnFamilyHandle = cfAddIfNotExist(cfName); //获取列族Handle
        try (RocksIterator rocksIterator = rocksDB.newIterator(columnFamilyHandle)) {
            for (rocksIterator.seekToFirst(); rocksIterator.isValid(); rocksIterator.next()) {
                count++;
            }
        }
        return count;
    }
}

