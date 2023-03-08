package io.dexterity.storage.service.impl;

import io.dexterity.common.client.RocksDBClient;
import io.dexterity.storage.po.vo.RocksDBVo;
import io.dexterity.storage.service.StorageService;
import org.rocksdb.RocksDBException;
import org.springframework.stereotype.Service;

import java.util.*;

@Service
public class StorageServiceImpl implements StorageService {

    public int cfAdd(String cfName) throws RocksDBException {
        RocksDBClient.cfAddIfNotExist(cfName);
        return 1;
    }

    public int cfDelete(String cfName) throws RocksDBException {
        RocksDBClient.cfDeleteIfExist(cfName);
        return 1;
    }

    public Set<String> cfAll(){
        return RocksDBClient.columnFamilyHandleMap.keySet();
    }

    public int put(RocksDBVo rocksDBVo) throws RocksDBException {
        RocksDBClient.put(rocksDBVo.getCfName(), rocksDBVo.getKey(), rocksDBVo.getValue());
        return 1;
    }

    public int putBatch(List<RocksDBVo> rocksDBVos) throws RocksDBException {
        Map<String, String> map = new HashMap<>();
        for (RocksDBVo rocksDBVo : rocksDBVos) {
            map.put(rocksDBVo.getKey(), rocksDBVo.getValue());
        }
        RocksDBClient.batchPut(rocksDBVos.get(0).getCfName(), map);
        return 1;
    }

    public RocksDBVo delete(String cfName, String key) throws RocksDBException {
        String value = RocksDBClient.get(cfName, key);
        RocksDBClient.delete(cfName, key);
        return RocksDBVo.builder().cfName(cfName).key(key).value(value).build();
    }

    public RocksDBVo get(String cfName, String key) throws RocksDBException {
        String value = RocksDBClient.get(cfName, key);
        return RocksDBVo.builder().cfName(cfName).key(key).value(value).build();
    }

    public List<RocksDBVo> multiGetAsList(List<RocksDBVo> rocksDBVos) throws RocksDBException {
        List<RocksDBVo> list = new ArrayList<>();
        String cfName = rocksDBVos.get(0).getCfName();
        List<String> keys = new ArrayList<>(rocksDBVos.size());
        for (RocksDBVo rocksDBVo : rocksDBVos) {
            keys.add(rocksDBVo.getKey());
        }
        Map<String, String> map = RocksDBClient.multiGetAsMap(cfName, keys);
        for (Map.Entry<String, String> entry : map.entrySet()) {
            RocksDBVo rocksDBVo = RocksDBVo.builder().cfName(cfName).key(entry.getKey()).value(entry.getValue()).build();
            list.add(rocksDBVo);
        }
        return list;
    }

    public List<RocksDBVo> getAll(String cfName) throws RocksDBException {
        List<RocksDBVo> rocksDBVos = new ArrayList<>();
        Map<String, String> all = RocksDBClient.getAll(cfName);
        for (Map.Entry<String, String> entry : all.entrySet()) {
            RocksDBVo rocksDBVo = RocksDBVo.builder().cfName(cfName).key(entry.getKey()).value(entry.getValue()).build();
            rocksDBVos.add(rocksDBVo);
        }
        return rocksDBVos;
    }

    public List<String> getKeysFrom(String cfName,String lastKey) throws RocksDBException {
        List<String> data = new ArrayList<>();
        List<String> keys;
        while (true) {
            keys = RocksDBClient.getKeysFrom(cfName, lastKey);
            if (keys.isEmpty()) {
                break;
            }
            lastKey = keys.get(keys.size() - 1);
            data.addAll(keys);
            keys.clear();
        }
        return data;
    }

    public List<String> getAllKey(String cfName) throws RocksDBException {
        return RocksDBClient.getAllKey(cfName);
    }

    public int getCount( String cfName) throws RocksDBException {
        return RocksDBClient.getCount(cfName);
    }
}
