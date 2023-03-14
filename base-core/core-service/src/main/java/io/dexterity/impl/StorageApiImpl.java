package io.dexterity.impl;

import io.dexterity.StorageApi;
import io.dexterity.po.vo.RocksDBVo;
import io.dexterity.service.StorageService;
import org.rocksdb.RocksDBException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Set;
@Service
public class StorageApiImpl implements StorageApi {
    @Autowired
    private StorageService storageService;
    @Override
    public int getCount(String cfName) throws RocksDBException {
        return storageService.getCount(cfName);
    }

    @Override
    public List<byte[]> getAllKey(String cfName) throws RocksDBException {
        return storageService.getAllKey(cfName);
    }

    @Override
    public List<byte[]> getKeysFrom(String cfName, byte[] lastKey) throws RocksDBException {
        return storageService.getKeysFrom(cfName,lastKey);
    }

    @Override
    public List<RocksDBVo> getAll(String cfName) throws RocksDBException {
        return storageService.getAll(cfName);
    }

    @Override
    public RocksDBVo get(String cfName, byte[] key) throws RocksDBException {
        return storageService.get(cfName,key);
    }

    @Override
    public RocksDBVo delete(String cfName, byte[] key) throws RocksDBException {
        return storageService.delete(cfName,key);
    }

    @Override
    public int putBatch(List<RocksDBVo> rocksDBVos) throws RocksDBException {
        return storageService.putBatch(rocksDBVos);
    }

    @Override
    public int put(RocksDBVo rocksDBVo) throws RocksDBException {
        return storageService.put(rocksDBVo);
    }

    @Override
    public Set<String> cfAll() {
        return storageService.cfAll();
    }

    @Override
    public int cfDelete(String cfName) throws RocksDBException {
        return storageService.cfDelete(cfName);
    }

    @Override
    public int cfAdd(String cfName) throws RocksDBException {
        return storageService.cfAdd(cfName);
    }
}
