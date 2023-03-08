package io.dexterity.service.api;

import io.dexterity.storage.po.vo.RocksDBVo;
import org.rocksdb.RocksDBException;

import java.util.List;
import java.util.Set;

public interface StorageApi {
    int getCount( String cfName) throws RocksDBException;
    List<String> getAllKey(String cfName) throws RocksDBException;
    List<String> getKeysFrom(String cfName,String lastKey) throws RocksDBException;
    List<RocksDBVo> getAll(String cfName) throws RocksDBException;
    List<RocksDBVo> multiGetAsList(List<RocksDBVo> rocksDBVos) throws RocksDBException;
    RocksDBVo get(String cfName, String key) throws RocksDBException;
    RocksDBVo delete(String cfName, String key) throws RocksDBException;
    int putBatch(List<RocksDBVo> rocksDBVos) throws RocksDBException;
    int put(RocksDBVo rocksDBVo) throws RocksDBException;
    Set<String> cfAll();
    int cfDelete(String cfName) throws RocksDBException;
    int cfAdd(String cfName) throws RocksDBException;
}
