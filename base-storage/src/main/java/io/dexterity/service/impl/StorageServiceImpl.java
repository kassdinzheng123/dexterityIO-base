package io.dexterity.service.impl;


import io.dexterity.po.pojo.RocksDBClient;
import io.dexterity.po.vo.RocksDBVo;
import io.dexterity.service.StorageService;
import org.rocksdb.RocksDBException;
import org.rocksdb.Transaction;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

@Service
public class StorageServiceImpl implements StorageService {

    public Integer cfAdd(String cfName, Transaction transaction) throws RocksDBException {
        RocksDBClient.cfAddIfNotExist(cfName);
        return 1;
    }


    public Integer cfDelete(String cfName, Transaction transaction) throws RocksDBException {
        RocksDBClient.cfDeleteIfExist(cfName);
        return 1;
    }

    public Set<String> cfAll(Transaction transaction){
        return RocksDBClient.columnFamilyHandleMap.keySet();
    }


//    public RocksIterator getIterator(String cfName, Transaction transaction) throws RocksDBException {
//        return RocksDBClient.getIterator(cfName);
//    }


    public Integer put(RocksDBVo rocksDBVo, Transaction transaction) throws RocksDBException {
        RocksDBClient.put(rocksDBVo.getCfName(), rocksDBVo.getKey(), rocksDBVo.getValue(),transaction);
        return 1;
    }

//    public int putBatch(List<RocksDBVo> rocksDBVos, Transaction transaction) throws RocksDBException {
//        Map<byte[], byte[]> map = new HashMap<>();
//        for (RocksDBVo rocksDBVo : rocksDBVos) {
//            map.put(rocksDBVo.getKey(), rocksDBVo.getValue());
//        }
//        RocksDBClient.batchPut(rocksDBVos.get(0).getCfName(), map);
//        return 1;
//    }

    public RocksDBVo delete(String cfName, byte[] key, Transaction transaction) throws RocksDBException {
        byte[] value = RocksDBClient.get(cfName, key ,transaction);
        RocksDBClient.delete(cfName, key,transaction);
        return RocksDBVo.builder().cfName(cfName).key(key).value(value).build();
    }

    public RocksDBVo get(String cfName, byte[] key, Transaction transaction) throws RocksDBException {
        byte[] value = RocksDBClient.get(cfName, key ,transaction);
        return RocksDBVo.builder().cfName(cfName).key(key).value(value).build();
    }

//    public List<RocksDBVo> getAll(String cfName, Transaction transaction) throws RocksDBException {
//        List<RocksDBVo> rocksDBVos = new ArrayList<>();
//        Map<byte[], byte[]> all = RocksDBClient.getAll(cfName,transaction,);
//        for (Map.Entry<byte[], byte[]> entry : all.entrySet()) {
//            RocksDBVo rocksDBVo = RocksDBVo.builder().cfName(cfName).key(entry.getKey()).value(entry.getValue()).build();
//            rocksDBVos.add(rocksDBVo);
//        }
//        return rocksDBVos;
//    }

    public List<byte[]> getKeysFrom(String cfName, byte[] lastKey, Transaction transaction) throws RocksDBException {
        List<byte[]> data = new ArrayList<>();
        List<byte[]> keys;
        while (true) {
            keys = RocksDBClient.getKeysFrom(cfName, lastKey, transaction);
            if (keys.isEmpty()) {
                break;
            }
            lastKey = keys.get(keys.size() - 1);
            data.addAll(keys);
            keys.clear();
        }
        return data;
    }

    public List<byte[]> getKeys(String cfName, Transaction transaction) throws RocksDBException {
        return RocksDBClient.getKeys(cfName, transaction);
    }

    public Integer getCount(String cfName, Transaction transaction) throws RocksDBException {
        return RocksDBClient.getCount(cfName, transaction);
    }
}
