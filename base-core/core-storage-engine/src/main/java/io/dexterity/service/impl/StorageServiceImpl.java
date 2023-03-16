package io.dexterity.service.impl;

import io.dexterity.client.RocksDBClient;
import io.dexterity.po.vo.RocksDBVo;
import io.dexterity.service.StorageService;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.TransactionDB;
import org.springframework.stereotype.Service;

import java.util.*;

@Service
public class StorageServiceImpl implements StorageService {

    public int cfAdd(String cfName) throws RocksDBException {
        RocksDBClient.cfAddIfNotExist(cfName);
        return 1;
    }

    @Override
    public RocksIterator getIterator(String chunkTmp) throws RocksDBException {
        return RocksDBClient.getIterator(chunkTmp);
    }

    @Override
    public TransactionDB getTransaction() throws RocksDBException {
        return RocksDBClient.openTransaction();
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
        Map<byte[], byte[]> map = new HashMap<>();
        for (RocksDBVo rocksDBVo : rocksDBVos) {
            map.put(rocksDBVo.getKey(), rocksDBVo.getValue());
        }
        RocksDBClient.batchPut(rocksDBVos.get(0).getCfName(), map);
        return 1;
    }

    public RocksDBVo delete(String cfName, byte[] key) throws RocksDBException {
        byte[] value = RocksDBClient.get(cfName, key);
        RocksDBClient.delete(cfName, key);
        return RocksDBVo.builder().cfName(cfName).key(key).value(value).build();
    }

    public RocksDBVo get(String cfName, byte[] key) throws RocksDBException {
        byte[] value = RocksDBClient.get(cfName, key);
        return RocksDBVo.builder().cfName(cfName).key(key).value(value).build();
    }

    public List<RocksDBVo> getAll(String cfName) throws RocksDBException {
        List<RocksDBVo> rocksDBVos = new ArrayList<>();
        Map<byte[], byte[]> all = RocksDBClient.getAll(cfName);
        for (Map.Entry<byte[], byte[]> entry : all.entrySet()) {
            RocksDBVo rocksDBVo = RocksDBVo.builder().cfName(cfName).key(entry.getKey()).value(entry.getValue()).build();
            rocksDBVos.add(rocksDBVo);
        }
        return rocksDBVos;
    }

    public List<byte[]> getKeysFrom(String cfName,byte[] lastKey) throws RocksDBException {
        List<byte[]> data = new ArrayList<>();
        List<byte[]> keys;
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

    public List<byte[]> getAllKey(String cfName) throws RocksDBException {
        return RocksDBClient.getAllKey(cfName);
    }

    public int getCount(String cfName) throws RocksDBException {
        return RocksDBClient.getCount(cfName);
    }
}
